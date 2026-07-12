# command-io

An experimental Rust runtime for high-performance applications, built around messages, effects, and event delivery instead of the standard `Future`/`poll`/`Waker` execution model.

The goal is not to build another Tokio-compatible executor. The goal is to explore a different architecture where the runtime is primarily:

- an effect interpreter;
- an event distributor;
- a small deterministic scheduler.

The current repository is a minimal, std-only MVP that validates the core execution model before introducing real kernel I/O, multi-core sharding, or protocol-specific layers.

## Architectural idea

The entire design revolves around one simple rule:

**The runtime delivers messages to state machines. State machines produce declarative effects. The runtime interprets those effects and, when they complete, generates new messages.**

In other words:

```text
Message
	-> handle()
	-> Effects
	-> Runtime
	-> I/O, timers, queues, event sources
	-> New messages
```

The runtime never executes application logic outside `handle()`.

The application never performs I/O directly.

All interaction with the outside world is described declaratively and interpreted by the runtime.

## Objectives

The long-term direction of the project is guided by these constraints:

- no `Future`, `poll`, `Waker`, or futures executor;
- no callback-oriented application model;
- no shared queues in the hot path;
- no locks in the hot path;
- no unbounded growth of queues or buffers;
- no dependency on dynamic allocation during normal request processing beyond preallocated runtime structures;
- completion-oriented I/O as the eventual systems boundary;
- a design that naturally fits thread-per-core execution;
- explicit backpressure through fixed-capacity resources;
- a runtime small enough to reason about locally.

This is an architecture exploration first, not a production-ready framework.

## Programming model

The basic unit of execution is an `Isolate`.

An isolate is a state machine that receives a message, updates its internal state, and emits a small set of declarative effects.

Conceptually:

```rust
trait Isolate {
		type Message;

		fn handle(
				&mut self,
				msg: RuntimeMessage<Self::Message>,
				effects: &mut TurnEffects<Self::Message>,
		) -> Result<(), RuntimeError>;
}
```

There is no `poll()` method.

There is no `Context`.

There is no future to drive.

The application only reacts to messages and describes what should happen next.

## Messages vs effects

The design makes a hard distinction between facts and intentions.

### Messages

Messages represent facts that have already happened.

Examples:

- `Init`
- `Accepted`
- `RecvCompleted`
- `WriteCompleted`
- `TimerFired`
- `Shutdown`

Messages are what isolates consume.

In the current codebase this distinction is represented by `RuntimeMessage<M>` in [src/effects/mod.rs](src/effects/mod.rs), which separates:

- runtime-internal messages like `Init`;
- user-level messages wrapped as `User(M)`.

### Effects

Effects represent intentions.

They describe what the runtime should do after `handle()` returns.

Examples:

- send a message to another isolate;
- send a message to the current isolate;
- destroy the current isolate;
- wait for an accept/recv/write/timer completion.

Effects do not execute immediately inside the isolate. They are interpreted later by the runtime.

## Current object model

This section maps the core architecture to the actual objects in the repository.

### `Handle`

Defined in [src/arena.rs](src/arena.rs).

A `Handle` is the stable external reference to an object stored inside an arena slot. It contains:

- a slot index;
- a generation counter.

The generation prevents stale references from silently pointing to a reused slot after removal and reinsertion.

### `Slot<T>`

Defined in [src/arena.rs](src/arena.rs).

A slot is the physical storage cell inside the arena. Each slot tracks:

- its current generation;
- whether it is occupied;
- whether it is marked as pending;
- the stored object itself.

This is the runtime's low-level storage primitive.

### `Arena<T>`

Defined in [src/arena.rs](src/arena.rs).

`Arena<T>` is a fixed-capacity container for homogeneous objects. Today it is the storage layer for isolates.

Responsibilities:

- preallocate slots up front;
- insert objects in O(1);
- remove objects in O(1);
- reuse freed slots through a free list;
- validate generational handles;
- expose limited lifecycle state through the pending flag.

This is the first step toward a runtime that does not rely on per-object heap allocation in the hot path.

### `ArenaError`

Defined in [src/arena.rs](src/arena.rs).

Represents storage-level failures such as:

- `Full`, when capacity is exhausted;
- `InvalidHandle`, when a handle does not point to a live object.

### `RuntimeMessage<M>`

Defined in [src/effects/mod.rs](src/effects/mod.rs).

Represents messages delivered to isolates.

Current variants:

- `Init`: automatically delivered when an isolate is spawned;
- `User(M)`: application-defined message payload.

This type makes runtime lifecycle messages explicit without forcing the application to mix them into its own domain enum.

### `Action<M>`

Defined in [src/effects/mod.rs](src/effects/mod.rs).

Represents immediate, non-suspending effects emitted during a turn.

Current variants:

- `Send { target, message }`
- `SendSelf { message }`
- `DestroySelf`

These are interpreted immediately after `handle()` returns.

### `Wait<M>`

Defined in [src/effects/mod.rs](src/effects/mod.rs).

Represents a suspending effect that will complete later and re-enter the isolate as a new message.

Current variants:

- `Accept { listener, completion }`
- `Recv { source, completion }`
- `Write { sink, completion }`
- `Timer { ticks, completion }`

This is the current effect IR for asynchronous boundaries.

### `TurnEffects<M>`

Defined in [src/effects/mod.rs](src/effects/mod.rs).

`TurnEffects<M>` is the per-turn builder used by isolates to describe what should happen next.

It enforces an important invariant:

- a turn may emit many immediate `Action`s;
- a turn may emit at most one `Wait`;
- once a wait is set, the turn is sealed.

This keeps the model small and makes suspend points explicit.

### `EffectsError`

Defined in [src/effects/mod.rs](src/effects/mod.rs).

Represents failures while building the current turn's effect set, such as:

- too many immediate actions;
- attempting to emit effects after the turn is sealed;
- attempting to set more than one wait.

### `WaitBackend<M>`

Defined in [src/io/mod.rs](src/io/mod.rs).

This trait abstracts the source of suspending completions.

Its job is to:

- accept submitted waits;
- poll for progress;
- requeue completion messages back into the scheduler queue.

This is the abstraction boundary that will eventually allow a real kernel-backed implementation.

### `FakeWaitBackend<M>`

Defined in [src/io/mod.rs](src/io/mod.rs).

This is the current test and simulation backend.

It fakes completion of:

- accepts;
- receives;
- writes;
- timers.

Its purpose is not realism. Its purpose is to validate scheduler semantics and the effect model before adding real systems integration.

### `Envelope<M>`

Defined in [src/runtime.rs](src/runtime.rs).

An `Envelope` is the scheduler-level routing unit. It binds together:

- the target `Handle`;
- the `RuntimeMessage<M>` to deliver.

The central queue stores envelopes, not bare messages.

### `Isolate`

Defined in [src/runtime.rs](src/runtime.rs).

This trait is the entire application-facing execution surface.

An isolate:

- owns its internal mutable state;
- receives one message at a time;
- emits effects through `TurnEffects`;
- never directly manipulates the scheduler or I/O backend.

### `Scheduler<I, B>`

Defined in [src/runtime.rs](src/runtime.rs).

`Scheduler` is the runtime core.

Responsibilities:

- store isolates in an arena;
- own the central message queue;
- inject `Init` on spawn;
- deliver envelopes to live isolates;
- poll the wait backend for progress;
- collect effects from a turn;
- invoke the interpreter that applies those effects.

The current scheduler is intentionally simple:

- single-threaded;
- single-shard;
- one central queue;
- no per-isolate inboxes.

This is a deliberate MVP choice, not the final intended topology.

### `EffectInterpreter`

Defined in [src/runtime.rs](src/runtime.rs).

`EffectInterpreter` takes the declarative output of a turn and turns it into concrete runtime state changes.

Today that means:

- enqueue outgoing messages;
- enqueue self-messages;
- destroy the current isolate;
- submit a wait to the backend.

This object is the bridge between the application-side state machine and the runtime-side machinery.

### `StepResult`

Defined in [src/runtime.rs](src/runtime.rs).

Represents what happened during one scheduler iteration:

- `Idle`
- `ProcessedOne`
- `DroppedInvalid`
- `AdvancedWaits`

It is a compact way to describe scheduler progress without exposing internal details.

### `RuntimeError`

Defined in [src/runtime.rs](src/runtime.rs).

Represents failures at the runtime boundary, including:

- arena errors;
- effect construction errors;
- message queue overflow;
- wait queue overflow.

## Runtime flow

The current runtime flow is:

1. an isolate is spawned into the arena;
2. the scheduler automatically enqueues `RuntimeMessage::Init` for that isolate;
3. the scheduler polls the wait backend for completions;
4. the scheduler pops one `Envelope` from the central queue;
5. the target isolate handles the message and writes into `TurnEffects`;
6. the scheduler extracts those effects;
7. the `EffectInterpreter` applies them;
8. waits complete later and re-enter the system as new messages.

This keeps application logic synchronous and local while preserving asynchronous behavior at the runtime boundary.

## Event sources

The intended architecture treats I/O, timers, cross-thread mailboxes, and simulated events as variations of the same concept: event sources.

Conceptually, sources may include:

- kernel I/O completions;
- timers;
- inter-scheduler mailboxes;
- signals;
- simulation drivers.

The scheduler should not care where an event came from. It should only care that the event eventually becomes a message for an isolate.

Today, [src/io/mod.rs](src/io/mod.rs) is the first abstraction step in that direction.

## Storage model

All live isolates currently reside in preallocated slots inside a fixed-capacity arena.

The intended benefits are:

- predictable memory usage;
- O(1) insertion and removal;
- explicit capacity limits;
- no shared ownership model in the hot path;
- easier backpressure by construction.

Longer term, the architecture may evolve toward multiple arenas, potentially one per isolate type or per scheduler shard.

## Scheduler model

The current implementation is deliberately smaller than the long-term vision.

### What exists today

- one scheduler;
- one thread;
- one central queue;
- one fake wait backend;
- no real networking;
- no thread-to-thread messaging.

### What the architecture is aiming toward

- thread-per-core execution;
- one scheduler per core;
- one event backend per scheduler;
- isolate ownership pinned to a scheduler;
- message-only communication across schedulers.

That direction is important, but it is not implemented yet.

## Backpressure

One of the core design goals is to avoid overload by construction rather than relying on hidden buffering.

The model assumes fixed-capacity resources such as:

- arenas;
- message queues;
- wait queues;
- buffers;
- backend submission/completion resources.

When capacity is exhausted, the runtime should fail explicitly, reject work, or apply a defined policy. It should not silently grow internal structures forever.

The current MVP already reflects part of this idea through fixed-capacity arena slots and bounded scheduler/wait queues.

## Timers and completions

Timers are not special in the model.

They are simply delayed message producers.

The same principle applies to I/O operations: a wait declaration includes the message that will be delivered on completion.

Conceptually:

```text
Wait::Timer { completion: Retry }
	-> backend progresses time
	-> Retry message is enqueued
	-> isolate handles Retry
```

This keeps completion handling uniform across different event sources.

## Current status

The repository already validates a meaningful slice of the architecture:

- generational handles;
- fixed-capacity arena storage;
- isolate spawning with automatic `Init`;
- inter-isolate messaging;
- self-messaging;
- self-destruction;
- one-wait-per-turn semantics;
- fake accept/recv/write/timer completions;
- unit tests for arena and runtime behavior.

The demo entry point in [src/main.rs](src/main.rs) shows a small isolate progressing through internal ticks, arming a fake wait, and tearing itself down.

## Roadmap

The most relevant next steps are:

1. define stale-message handling so old in-flight work cannot violate isolate state transitions;
2. define failure semantics per isolate, including panic handling and teardown policy;
3. clean up warning strategy for intentionally staged APIs versus test-only helpers;
4. stabilize the effect IR before wiring in real kernel-facing backends;
5. add a real event backend, likely starting from a narrow completion-oriented interface;
6. design explicit backpressure policies for queue overflow and resource exhaustion;
7. evolve lifecycle and ownership rules for cancellation and invalidation of pending work;
8. expand tests around stale work, chained waits, and scheduler invariants;
9. only after the model is stable, move toward thread-per-core sharding.

## Build and run

```bash
cargo run
```

## Test

```bash
cargo test
```