use super::*;
use std::{pin::pin, task::ready};
use tokio::sync::watch;

/// An [`Address`] that is not yet running, but may be in the future.
pub struct SupervisionAddress<T: InboxKind> {
    receiver: watch::Receiver<Option<Address<T>>>,
}

pub(super) type FutureAddressSender<T> = watch::Sender<Option<Address<T>>>;

impl<T: InboxKind> SupervisionAddress<T> {
    pub(super) fn new() -> (Self, FutureAddressSender<T>) {
        let (tx, rx) = watch::channel(None);
        (Self { receiver: rx }, tx)
    }

    pub fn get_cloned(&self) -> Option<Address<T>> {
        self.receiver.borrow().clone()
    }
}

impl<T: InboxKind> Debug for SupervisionAddress<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisionAddress").finish()
    }
}

impl<T: InboxKind> Clone for SupervisionAddress<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
        }
    }
}

impl<T: InboxKind> Future for SupervisionAddress<T> {
    type Output = Option<Address<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let res = ready!(pin!(self.receiver.wait_for(|x| x.is_some())).poll_unpin(cx));

        Poll::Ready(
            res.ok()
                .map(|x| x.clone().expect("Wait for address that is some")),
        )
    }
}
