# command-io

An experimental Rust runtime for high-performance I/O, built on explicit
stepping, messages and effects instead of the standard
`Future`/`poll`/`Waker` model.

There is no hidden executor and no wake-up. The outer loop *is* the scheduler,
and every piece of progress happens because some `step` was called:

```rust
loop {
    server.step(&mut io)?; // owners submit work or consume ready results
    io.step()?;            // flush SQEs, retire available CQEs, never block
}
```

This makes the runtime deterministic, easy to instrument, and easy to drive
from tests or a simulator.

## Status

What exists today is the bottom layer: a single-threaded, Linux-only
(io_uring) file-read path.

| Module | Role |
| --- | --- |
| [`completion`](src/completion.rs) | Fixed-capacity arena of completion slots, addressed by generational `CompletionHandle`s. Each slot owns the resources the kernel can touch (file, buffer) and records the owner allowed to drive it. Lifecycle: `Idle -> Submitted -> Ready(result) -> Idle`. |
| [`io`](src/io/mod.rs) | `IoContext`: `submit` queues an SQE tagged with the handle in `user_data`; `step` flushes, drains available CQEs, marks slots `Ready`, and pushes handles onto a bounded ready queue. |
| [`reader`](src/reader.rs) | A hand-written owner state machine (`Start -> Reading -> Done/Failed`). |
| [`main`](src/main.rs) | A tiny `Server` that steps readers and routes ready completions to their owners. |

```sh
cargo test
cargo run                 # reads Cargo.toml and README.md concurrently
cargo run -- some/file    # or any files you pass
```

The design notes for this layer are in [docs/plan.md](docs/plan.md).

## Next: Isolates (design, not implemented)

The bottom layer is fast and testable, but writing against it is tedious. The
`Reader` has to acquire a slot, submit it, remember which handle it is waiting
on, check that a routed handle is the expected one, take the result, and reach
back into `IoContext` for its buffer. The `Server` has to route handles by
owner index. Every new state machine repeats all of that.

The proposal is an **Isolate**: a unit of state that behaves like a reactor or
effect handler. It owns its state machine, defines its own message type, and
reacts to one message at a time by emitting effects. It never touches
`IoContext`, slots, or handles.

### Shape

```rust
trait Isolate {
    /// Every message this isolate understands, including completed I/O.
    type Msg;

    /// One turn: react to a single message and record effects.
    fn handle(&mut self, msg: Self::Msg, fx: &mut Effects<Self::Msg>);
}
```

`Effects<Msg>` is a bounded buffer of *requests*. Nothing in it runs during
the turn. The runtime interprets the effects after `handle` returns:

- `fx.read(fd, buf, offset, Msg::Read)` submits I/O. The last argument is a
  plain `fn(ReadDone) -> Msg`, which tells the runtime how to turn the
  completion back into this isolate's own message type.
- `fx.send(addr, msg)` delivers a message to another isolate.
- `fx.spawn(isolate)` starts a new isolate.
- `fx.stop()` ends this isolate.

Buffers move into the operation and come back inside the completion, as with
"owned buffer" io_uring APIs, so the isolate never borrows memory the kernel
is writing into:

```rust
struct ReadDone {
    result: io::Result<usize>,
    buf: Buf, // the same buffer that was submitted
}
```

### Example

The `Reader` from `src/reader.rs`, rewritten to read a whole file:

```rust
enum Msg {
    Start,
    Read(ReadDone),
}

struct Cat {
    fd: Fd,
    offset: u64,
}

impl Isolate for Cat {
    type Msg = Msg;

    fn handle(&mut self, msg: Msg, fx: &mut Effects<Msg>) {
        match msg {
            Msg::Start => fx.read(self.fd, Buf::with_capacity(4096), self.offset, Msg::Read),
            Msg::Read(done) => match done.result {
                Ok(0) | Err(_) => fx.stop(),
                Ok(n) => {
                    self.offset += n as u64;
                    fx.read(self.fd, done.buf, self.offset, Msg::Read);
                }
            },
        }
    }
}
```

The tuple variant `Msg::Read` already is a `fn(ReadDone) -> Msg`, so the
isolate's enum stays the single list of everything it can receive.

### What the runtime does

The runtime layer sits on top of the current primitives and does the
bookkeeping that the `Reader` and `Server` do by hand today:

```text
loop {
    io.step()            // CQEs -> slots Ready -> ready queue        (exists)
    rt.route_ready(io)   // per ready handle: look up (isolate, map fn),
                         // take_result, release slot, build Msg,
                         // push into that isolate's mailbox
    rt.run_turns(budget) // pop one Msg per isolate, call handle()
    rt.apply(io)         // Submit -> acquire slot, record (isolate, map fn), io.submit
                         // Send -> push to target mailbox
                         // Spawn / Stop -> isolate arena
}
```

Mapping these onto the existing pieces:

- `OwnerHandle` becomes the isolate's generational address in an isolate
  arena, so the runtime can route completions without scanning.
- Completion slots become an implementation detail. The runtime acquires one
  per `Submit` and releases it when the completion is delivered.
- Every queue (effects, mailboxes, ready queue) is bounded and allocated up
  front, as in the current layer.

### Why effects instead of direct calls

- **Testing without a kernel.** `handle` is a function of
  `(state, msg) -> (state', effects)`. A test sends `Msg::Start`, checks that
  a `Read` effect was emitted, then calls its map function with a fake
  `ReadDone` and sends the resulting message back in. No io_uring, no timing.
- **Simulation.** A simulated backend only has to interpret the same effects
  and inject completions in any order it likes, including error paths and
  reorderings that are hard to reproduce with a real kernel.
- **One place for policy.** Backpressure, per-isolate slot quotas,
  cancellation and tracing all live in the interpreter, not in every state
  machine.

The cost is that nothing is synchronous: an isolate cannot submit a read and
learn in the same turn that the arena is full. Every outcome, including
rejection, arrives later as a message.

### Open questions

1. **Map function or token.** The alternative to a `fn(Completion) -> Msg` is
   an isolate-defined `type Token: Copy`, with completions delivered as
   `Event::Io(token, result)`. Tokens are easier to compare in tests. Map
   functions keep a single message enum.
2. **Resource ownership.** Should file descriptors stay owned by the isolate
   (`File` inside its state) or become runtime-owned resources referenced by
   handle, like slots? Runtime ownership makes `Stop` with operations in
   flight safe by construction.
3. **Stopping with I/O in flight.** The options are to orphan the slots
   (keep the buffers alive until the CQE arrives, then drop it) or to also
   issue an `AsyncCancel`. Either way the runtime, not the isolate, must hold
   the resources.
4. **Rejection as a message.** When a submission hits `Full`, the plan is to
   deliver `Err(Full)` through the same map function rather than fail the
   turn. That needs an error type wider than `io::Error`.
5. **Addresses and spawn.** Does `fx.spawn` return a typed `Addr<M>`
   immediately, which means reserving an arena slot during the turn, or only
   after the effect is applied?
6. **Isolate storage.** The choice is between `Box<dyn AnyIsolate>` (open set,
   one virtual call per message) and a user-defined enum of isolate kinds
   (closed set, no indirection).
7. **Fairness.** Should a turn handle one message per isolate per loop
   iteration, or drain up to a budget? What happens when a mailbox is full:
   drop, reject to the sender, or apply backpressure?

## Out of scope for now

Sockets, timers, multi-core sharding and non-Linux backends. The plan is to
get the isolate layer working on file reads first, then add more operation
kinds behind the same `Effects` interface.
