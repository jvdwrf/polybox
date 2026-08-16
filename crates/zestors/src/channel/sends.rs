use super::*;
use crate::Message;
use std::future::Future;

/// Provides message-sending operations for a channel.
///
/// `Sends` exposes four levels of delivery semantics:
///
/// - [`Sends::send`] applies backpressure and asynchronously waits when the
///   channel is under load.
/// - [`Sends::try_send`] applies backpressure but never waits, returning
///   [`ClosedOrFull::Full`] when the channel is under backpressure.
/// - [`Sends::send_now`] checks whether the channel is open but ignores
///   backpressure.
/// - [`Sends::force_send`] ignores both backpressure and the channel status.
pub trait Sends<M: Message>: Sync {
    /// Sends a message, applying backpressure when the channel is under load.
    ///
    /// This method waits asynchronously while backpressure is active. It
    /// returns [`Closed`] if the channel is closed.
    ///
    /// Unlike [`Sends::try_send`], this method waits rather than returning
    /// immediately when backpressure is active.
    fn send(&self, msg: M) -> impl Future<Output = Result<M::Output, SendError<M>>> + Send;

    /// Attempts to send a message without waiting.
    ///
    /// Returns [`ClosedOrFull::Full`] if the channel has reached its
    /// backpressure limit, or [`ClosedOrFull::Closed`] if the channel is
    /// closed.
    ///
    /// Unlike [`Sends::send`], this method never waits for backpressure to
    /// subside.
    fn try_send(&self, msg: M) -> Result<M::Output, TrySendError<M>>;

    /// Sends a message immediately if the channel is open.
    ///
    /// This method ignores backpressure, but still checks whether the channel
    /// is accepting messages. It returns [`Closed`] if the channel is closed.
    ///
    /// Use [`Sends::force_send`] when the channel status should also be
    /// ignored.
    fn send_now(&self, msg: M) -> Result<M::Output, SendError<M>>;

    /// Sends a message immediately, ignoring backpressure and channel status.
    ///
    /// This is the lowest-level sending operation. The message is queued even
    /// when the channel is closed.
    ///
    /// If the underlying queue is at capacity, the message is dropped and the
    /// implementation may log the overflow.
    fn force_send(&self, msg: M) -> M::Output;
}

impl<M, T> Sends<M> for T
where
    T: AsActorRef + Sync,
    M: Message,
    Channel<T::QueueType>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<<M as Message>::Output, SendError<M>> {
        self.as_channel().send(msg).await
    }

    fn try_send(&self, msg: M) -> Result<<M as Message>::Output, TrySendError<M>> {
        self.as_channel().try_send(msg)
    }

    fn send_now(&self, msg: M) -> Result<<M as Message>::Output, SendError<M>> {
        self.as_channel().send_now(msg)
    }

    fn force_send(&self, msg: M) -> <M as Message>::Output {
        self.as_channel().force_send(msg)
    }
}
