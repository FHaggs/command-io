#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeMessage<M> {
    Init,
    User(M),
}
