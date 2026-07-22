use super::*;
use std::future::Future;

pub trait Sends<T: Message> {
    fn send(&self, msg: T) -> impl Future<Output = Result<Output<T>, SendError<T>>> + Send + '_;

    fn send_blocking(&self, msg: T) -> Result<Output<T>, SendError<T>> {
        futures::executor::block_on(self.send(msg))
    }
}

pub trait SendsExt<T: Message>: Sends<T> {
    fn request(
        &self,
        msg: T,
    ) -> impl Future<Output = Result<Reply<T>, RequestError<T>>> + Send + '_ {
        let fut = self.send(msg);

        async { Ok(fut.await?.get().await?) }
    }

    fn request_blocking(&self, msg: T) -> Result<Reply<T>, RequestError<T>> {
        Ok(self.send_blocking(msg)?.get_blocking()?)
    }
}
