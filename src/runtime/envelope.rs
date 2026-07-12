use crate::arena::Handle;
use crate::effects::{OpToken, RuntimeMessage};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope<M> {
    pub target: Handle,
    pub message: RuntimeMessage<M>,
    /// Set when this envelope is the completion of a submitted operation, so
    /// the receiving isolate can correlate it with the wait it armed.
    pub token: Option<OpToken>,
}
