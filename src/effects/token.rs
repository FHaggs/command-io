/// Correlation token minted by the runtime for each submitted [`Wait`].
///
/// The token is returned to the isolate when it arms a wait and is echoed back
/// on the completion message so the isolate can match a completion to the
/// operation that produced it.
///
/// [`Wait`]: super::wait::Wait
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OpToken(pub u32);
