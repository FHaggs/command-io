use crate::effects::{OpToken, Wait};
use crate::runtime::RuntimeError;

/// Source of suspending completions.
///
/// A backend only tracks *tokens* and the resources they wait on. It does not
/// know about isolates or completion messages; the scheduler's wait registry
/// owns that state. When an operation is ready, `poll` reports its token and the
/// scheduler decides what message to deliver and where.
pub trait WaitBackend {
    /// Arm an operation identified by `token`.
    fn submit(&mut self, token: OpToken, wait: Wait) -> Result<(), RuntimeError>;

    /// Cancel a previously armed operation. Unknown tokens are ignored.
    fn cancel(&mut self, token: OpToken);

    /// Fire any ready operations, appending their tokens to `ready`. Returns
    /// `true` if any progress was made (a completion fired or a timer ticked).
    fn poll(&mut self, ready: &mut Vec<OpToken>) -> Result<bool, RuntimeError>;

    fn has_pending(&self) -> bool;

    /// Whether the backend can accept `additional` more submitted waits.
    ///
    /// Used by the interpreter to validate capacity before applying a turn's
    /// effects, so a turn either applies fully or not at all.
    fn can_submit(&self, additional: usize) -> bool;

    /// Block until an external event becomes available, returning `true` when
    /// the caller should poll again.
    ///
    /// The default implementation performs no blocking and returns `false`,
    /// meaning "no external event system can make progress". Backends that own
    /// a real event source (kernel completion queue, simulation clock) override
    /// this to actually wait and return `true` once an event is ready.
    fn block_until_event(&mut self) -> Result<bool, RuntimeError> {
        Ok(false)
    }
}
