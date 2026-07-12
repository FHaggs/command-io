use crate::arena::ArenaError;
use crate::effects::EffectsError;

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeError {
    Arena(ArenaError),
    Effects(EffectsError),
    QueueFull,
    WaitQueueFull,
}

impl From<ArenaError> for RuntimeError {
    fn from(value: ArenaError) -> Self {
        Self::Arena(value)
    }
}

impl From<EffectsError> for RuntimeError {
    fn from(value: EffectsError) -> Self {
        Self::Effects(value)
    }
}
