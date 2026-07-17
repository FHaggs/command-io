# command-io

An experimental Rust runtime for high-performance applications, built around messages, effects, and event delivery instead of the standard `Future`/`poll`/`Waker` execution model.

The goal is to explore a different architecture where the runtime is primarily:

- an effect interpreter;
- an event distributor;
- a small deterministic scheduler.

The current repository is a minimal, std-only MVP that validates the core execution model before introducing real kernel I/O, multi-core sharding, or protocol-specific layers.
