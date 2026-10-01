# Hand-Implementation Guide: Explicit I/O Stepping

## Core model

There are three separate roles:

1. **Owner/state machine** — decides which operation to start and what state
   transition to perform after it finishes.
2. **Completion slot** — persistent mailbox for exactly one operation:
   `Idle -> Submitted -> Ready -> Idle`.
3. **I/O engine** — translates submissions to SQEs and CQEs to results. It
   never advances application state.

There is no wake-up. The outer loop is the scheduler:

```text
loop {
    server.step(&mut io)?; // submit work or consume ready results
    io.step()?;            // submit SQEs and retire available CQEs
}
```

If `io.step()` completes a read, the read owner does not run immediately.
`io.step()` only marks the completion `Ready`. On the next loop iteration,
`server.step()` observes that fact and advances the owner.

## Do you need a completion handle?

For the current design, use one.

Passing a raw pointer to a caller-owned `Completion` through
`Entry::user_data` requires the completion and all referenced file/buffer
storage to stay at a stable address until the CQE arrives. Moving values
between `queued` and `in_flight` `Vec`s makes that contract difficult to
guarantee safely.

Use a generational handle instead:

```text
CompletionHandle {
    index: u32,
    generation: u32,
}
```

Encode it into `u64` for `Entry::user_data`:

```text
raw = (generation << 32) | index
```

On completion, decode `cqe.user_data()`, look up the slot, and reject a stale
generation. The handle identifies the completion; it is not the result and it
does not itself own memory.

A direct pointer can be explored later when slots are pinned and their
lifetime contract is deliberately unsafe. A handle-backed fixed arena is the
better first implementation.

## Who owns the completion?

Separate **logical ownership** from **physical storage**:

- `IoContext`/`CompletionArena` physically stores fixed-capacity slots so their
  operation resources remain alive.
- Each slot records an `OwnerHandle` identifying the application state machine
  that acquired it.
- Only that owner may submit, take the result, reset, or release the slot.
- `IoEngine` may complete a submitted slot, but cannot consume its result or
  choose the owner's next state.

For one reader, `OwnerHandle` can initially be a small ID. When owners live in
an arena, make it another generational `{ index, generation }` handle.

The completion slot, not a temporary stack frame, must retain every resource
the kernel can still touch:

- operation kind,
- raw file descriptor whose owning `File` remains alive,
- read buffer,
- lifecycle state,
- terminal `io::Result<usize>`.

## How completion reaches its owner

Choose explicit routing rather than scanning every owner:

```text
IoContext {
    engine,
    completions: CompletionArena,
    ready: fixed-capacity queue<CompletionHandle>,
}
```

Submission:

1. Owner acquires a completion handle once and keeps it in its own state.
2. Owner prepares the slot's operation and buffer.
3. `IoContext::submit(owner, completion)` verifies ownership and `Idle`.
4. Set the slot to `Submitted`.
5. Build the SQE with `user_data(completion.into_raw())`.
6. Queue it without waiting, then return to the caller.

I/O step:

1. Flush queued SQEs to the kernel.
2. Drain only CQEs currently available; do not spin and do not call a wait
   method.
3. Decode each CQE's completion handle.
4. Validate that the slot exists and is `Submitted`.
5. Store `Ok(bytes)` or the decoded OS error.
6. Change the slot to `Ready`.
7. Push the completion handle into the bounded `ready` queue.

Server step:

1. Pop a ready completion handle.
2. Ask the completion arena for its recorded owner.
3. Route `IoCompleted(completion_handle)` to that owner, or directly invoke one
   turn of that owner's state machine.
4. The owner verifies that the handle is the one it expects.
5. The owner calls `take_result(owner, completion)`.
6. `take_result` returns the terminal result and resets the slot to `Idle`.
7. The owner performs its application transition and may reuse the slot.

This ready queue is not a waker or hidden executor. It is explicit data
produced by `io.step()` and consumed only when `server.step()` is called.

## Suggested implementation order

1. **Remove `Entry` from `Completion`.** Store a semantic read operation and
   its resources instead. Kernel-specific `Entry` values should be built when
   submitting.
2. **Implement `CompletionHandle` encode/decode** and a fixed-capacity
   completion arena with generation checks.
3. **Reduce lifecycle states** to `Idle`, `Submitted`, and `Ready`.
   Submission failures return the slot to `Idle`; both success and I/O failure
   become `Ready` because both are terminal results the owner must consume.
4. **Put the handle in `user_data`.** This is the missing association in the
   current `pool_one`; without it, a CQE cannot be matched to an operation or
   owner.
5. **Make `IoContext::step` nonblocking.** Submit queued SQEs, drain available
   CQEs, mark slots ready, and populate the ready queue.
6. **Write a tiny `Reader` state machine** with states such as `Start`,
   `Reading { completion }`, `Done`, and `Failed`. Its `step` submits in
   `Start`, does nothing while waiting, and consumes the result only after the
   matching completion is routed back.
7. **Drive it from the outer loop** by alternating the reader/server step and
   I/O step.

## Corrections to the current draft

- `IoContext::step` currently submits requests but never harvests CQEs.
- `IoEngine::submit` calls `ring.submit()` immediately; move that progress into
  the explicit I/O step.
- `pool_one` returns only a byte count/error and discards `cqe.user_data()`, so
  it cannot identify an operation or owner.
- `Pending` and `InProgress` overlap with queue placement. Prefer the slot
  lifecycle `Idle/Submitted/Ready`; keep `queued/in-flight/ready` as routing
  structures, not extra semantic states.
- `Completed` and `Failed` should not be separate lifecycle states. Store
  `Result<usize, io::Error>` in one `Ready` state.
- Moving whole completion values from `queued` to `in_flight` obscures stable
  ownership. Keep slots in one arena and move only handles through queues.
- The `File` and read buffer in the unfinished test are local variables. They
  must live until the matching CQE has been retired.
- Avoid `drain(..)` plus unbounded `Vec::push` on the hot path. Allocate fixed
  capacities up front and return an explicit `Full` error when bounded queues
  are exhausted.

## First tests to write

1. A handle round-trips through `u64`.
2. Releasing and reacquiring a slot changes its generation.
3. The wrong owner cannot submit or take a result.
4. A submitted slot cannot be submitted again or released.
5. A CQE result is stored in the slot selected by `user_data`.
6. The reader remains in `Reading` until `io.step()` retires its CQE.
7. A successful result returns bytes and resets the slot to `Idle`.
8. A negative CQE becomes `Ready(Err(...))` and is delivered to the same owner.
9. Queue/arena exhaustion returns `Full` without allocating or panicking.

## Scope

Implement only one file-read operation first. Keep sockets, cancellation,
isolate destruction, simulation, and backend traits out of this pass. Once the
native state machine works, the simulation backend can implement the same
submission/step/result contract without changing the `Reader`.
