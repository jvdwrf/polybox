use super::*;
use std::future::Future;

/// Implemented for inboxes that can send messages of type `T`.
pub trait Sends<M: Message> {
    /// Sends a message of type `T` to the inbox.
    fn send(&self, msg: M) -> impl Future<Output = Result<M::Output, SendError<M>>> + Send + '_;

    /// Same as [`Sends::send`], but blocks the current thread until the message is sent.
    fn send_blocking(&self, msg: M) -> Result<M::Output, SendError<M>> {
        futures::executor::block_on(self.send(msg))
    }
}

/// Extension trait for [`Sends`].
pub trait SendsExt<M: Message>: Sends<M> {
    /// Sends a message of type `T` to the inbox and waits for a reply, if the message type has a reply type.
    ///
    /// This is equivalent to calling [`Sends::send`] and then [`MessageReply::receive`].
    fn request(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Reply, RequestError<M>>> + Send + '_ {
        let fut = self.send(msg);

        async { Ok(fut.await?.receive().await?) }
    }

    /// Same as [`SendsExt::request`], but blocks the current thread until the reply is received.
    fn request_blocking(&self, msg: M) -> Result<M::Reply, RequestError<M>> {
        Ok(self.send_blocking(msg)?.receive_blocking()?)
    }
}
impl<T: Message, S: Sends<T>> SendsExt<T> for S {}
