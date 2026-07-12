use crate::effects::{OpToken, RuntimeMessage, TurnEffects};

use super::error::RuntimeError;

pub trait Isolate {
    type Message;

    fn handle(
        &mut self,
        msg: RuntimeMessage<Self::Message>,
        token: Option<OpToken>,
        effects: &mut TurnEffects<Self::Message>,
    ) -> Result<(), RuntimeError>;
}
