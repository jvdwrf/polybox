use super::*;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, Hash)]
pub enum TrySendError<T> {
    #[error("Channel is closed")]
    Closed(T),

    #[error("Channel is full")]
    Full(T),
}

impl<T> TrySendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Closed(t) => t,
            TrySendError::Full(t) => t,
        }
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, Hash)]
#[error("Channel is closed")]
pub struct SendError<T>(pub T);

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, Hash)]
pub enum SendCheckedError<T> {
    #[error("Channel is closed")]
    Closed(T),

    #[error("Message type not accepted by channel")]
    NotAccepted(T),
}

impl<T> SendCheckedError<T> {
    pub fn into_inner(self) -> T {
        match self {
            SendCheckedError::Closed(t) => t,
            SendCheckedError::NotAccepted(t) => t,
        }
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, Hash)]
pub enum TrySendCheckedError<T> {
    #[error("Channel is closed")]
    Closed(T),

    #[error("Channel is full")]
    Full(T),

    #[error("Message type not accepted by channel")]
    NotAccepted(T),
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, Hash)]
#[error("Message type not accepted by channel")]
pub struct NotAccepted<T>(pub T);

impl<T> TrySendCheckedError<T> {
    pub fn into_inner(self) -> T {
        match self {
            TrySendCheckedError::Closed(t) => t,
            TrySendCheckedError::Full(t) => t,
            TrySendCheckedError::NotAccepted(t) => t,
        }
    }
}

impl<T> From<SendError<T>> for TrySendError<T> {
    fn from(err: SendError<T>) -> Self {
        TrySendError::Closed(err.0)
    }
}

impl<T> From<PushError<T>> for TrySendError<T> {
    fn from(err: PushError<T>) -> Self {
        match err {
            PushError::Closed(t) => TrySendError::Closed(t),
            PushError::Full(t) => TrySendError::Full(t),
        }
    }
}

impl<T> From<SendCheckedError<T>> for TrySendCheckedError<T> {
    fn from(err: SendCheckedError<T>) -> Self {
        match err {
            SendCheckedError::Closed(t) => TrySendCheckedError::Closed(t),
            SendCheckedError::NotAccepted(t) => TrySendCheckedError::NotAccepted(t),
        }
    }
}

impl<T> From<PushError<T>> for TrySendCheckedError<T> {
    fn from(err: PushError<T>) -> Self {
        match err {
            PushError::Closed(t) => TrySendCheckedError::Closed(t),
            PushError::Full(t) => TrySendCheckedError::Full(t),
        }
    }
}

impl<T> From<TrySendError<T>> for TrySendCheckedError<T> {
    fn from(err: TrySendError<T>) -> Self {
        match err {
            TrySendError::Closed(t) => TrySendCheckedError::Closed(t),
            TrySendError::Full(t) => TrySendCheckedError::Full(t),
        }
    }
}
