use super::*;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, Hash)]
pub enum ClosedOrFull<T> {
    #[error("Channel is closed")]
    Closed(T),

    #[error("Channel is full")]
    Full(T),
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, Hash)]
#[error("Channel is closed")]
pub struct Closed<T>(pub T);

impl<T> From<Closed<T>> for ClosedOrFull<T> {
    fn from(err: Closed<T>) -> Self {
        ClosedOrFull::Closed(err.0)
    }
}

impl<T> From<PushError<T>> for ClosedOrFull<T> {
    fn from(err: PushError<T>) -> Self {
        match err {
            PushError::Closed(t) => ClosedOrFull::Closed(t),
            PushError::Full(t) => ClosedOrFull::Full(t),
        }
    }
}
