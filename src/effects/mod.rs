use crate::completion::CompletionHandle;
use crate::io::Operation;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum RuntimeMessage {
    Init,
    IoCompleted(CompletionHandle),
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Submit {
        completion: CompletionHandle,
        op: Operation,
    },
    Cancel {
        completion: CompletionHandle,
    },
    DestroySelf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectsError {
    Full,
}

pub struct TurnEffects {
    effects: Vec<Effect>,
    max_effects: usize,
}

impl TurnEffects {
    pub fn with_capacity(max_effects: usize) -> Self {
        Self {
            effects: Vec::with_capacity(max_effects),
            max_effects,
        }
    }

    pub fn reset(&mut self) {
        self.effects.clear();
    }

    pub fn submit(
        &mut self,
        completion: CompletionHandle,
        op: Operation,
    ) -> Result<(), EffectsError> {
        self.push(Effect::Submit { completion, op })
    }

    pub fn cancel(&mut self, completion: CompletionHandle) -> Result<(), EffectsError> {
        self.push(Effect::Cancel { completion })
    }

    pub fn destroy_self(&mut self) -> Result<(), EffectsError> {
        self.push(Effect::DestroySelf)
    }

    pub fn swap_effects(&mut self, scratch: &mut Vec<Effect>) {
        scratch.clear();
        std::mem::swap(&mut self.effects, scratch);
    }

    fn push(&mut self, effect: Effect) -> Result<(), EffectsError> {
        if self.effects.len() == self.max_effects {
            return Err(EffectsError::Full);
        }

        self.effects.push(effect);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Effect, EffectsError, TurnEffects};
    use crate::completion::CompletionHandle;
    use crate::io::{Operation, TimerOp};

    #[test]
    fn effects_are_bounded_and_preserve_order() {
        let mut effects = TurnEffects::with_capacity(2);
        let completion = CompletionHandle::INVALID;

        effects
            .submit(completion, Operation::Timer(TimerOp { ticks: 1 }))
            .unwrap();
        effects.cancel(completion).unwrap();
        assert_eq!(effects.destroy_self(), Err(EffectsError::Full));

        let mut scratch = Vec::with_capacity(2);
        effects.swap_effects(&mut scratch);
        assert!(matches!(scratch[0], Effect::Submit { .. }));
        assert!(matches!(scratch[1], Effect::Cancel { .. }));
    }
}
