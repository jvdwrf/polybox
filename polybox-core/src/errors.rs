use crate::RxError;
use thiserror::Error;

/// Error returned when sending a message, checking at compile-time that
/// the message is actually accepted by the actor.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, Hash)]
pub enum SendCheckedError<M> {
    Closed(M),
    NotAccepted(M),
}

/// The channel has been closed, and no longer accepts new messages.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
pub struct SendError<M>(pub M);

/// Error returned when sending a request.
///
/// This error combines failures in sending and receiving.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum RequestError<M> {
    NoReply,
    Closed(M),
}

impl<T> From<SendError<T>> for SendCheckedError<T> {
    fn from(err: SendError<T>) -> Self {
        Self::Closed(err.0)
    }
}

impl<T> From<RxError> for RequestError<T> {
    fn from(RxError: RxError) -> Self {
        Self::NoReply
    }
}

impl<T> From<SendError<T>> for RequestError<T> {
    fn from(err: SendError<T>) -> Self {
        Self::Closed(err.0)
    }
}
