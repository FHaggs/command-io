/// A resource descriptor for a suspending operation.
///
/// A `Wait` only names *what* to wait on. The completion message and the
/// isolate that armed it are tracked by the runtime's wait registry, not here,
/// so a wait can be moved between the effect layer, the registry, and the
/// backend without carrying application state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wait {
    Accept { listener: u32 },
    Recv { source: u32 },
    Write { sink: u32 },
    Timer { ticks: u32 },
}
