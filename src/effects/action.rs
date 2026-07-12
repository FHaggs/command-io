use crate::arena::Handle;

use super::message::RuntimeMessage;
use super::token::OpToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action<M> {
    Send {
        target: Handle,
        message: RuntimeMessage<M>,
    },
    SendSelf {
        message: M,
    },
    DestroySelf,
    /// Cancel a previously armed operation by its token. Cancelling an
    /// already-completed or unknown token is a no-op.
    Cancel {
        token: OpToken,
    },
}
