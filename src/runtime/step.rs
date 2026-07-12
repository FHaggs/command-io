#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepResult {
    Idle,
    ProcessedOne,
    DroppedInvalid,
    AdvancedWaits,
    /// The queue is empty and at least one wait is armed, but polling made no
    /// progress this step. The runtime must block for an external event rather
    /// than busy-spin.
    Blocked,
}
