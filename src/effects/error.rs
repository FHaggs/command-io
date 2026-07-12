#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectsError {
    /// The per-turn action buffer is full.
    ActionsFull,
    /// The per-turn wait buffer is full.
    WaitsFull,
}
