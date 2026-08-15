use crate::_prelude::*;

// pub struct TestProcessDataWrapper<T: SenderKind> {
//     inner: Arc<TestProcessData<T>>,
// }

// impl<T: Interface> TestProcessDataWrapper<T> {
//     pub fn into_any(self) -> TestProcessDataWrapper<Set!()> {
//         TestProcessDataWrapper {
//             inner: self.inner as Arc<TestProcessData<Set!()>>,
//         }
//     }
// }

pub struct TestProcessData<T: SenderKind> {
    data: SharedProcessDataInner,
    sender: T::Sender,
}

#[derive(Debug, Clone)]
pub(crate) struct SharedProcessData {
    inner: Arc<SharedProcessDataInner>,
}

#[derive(Debug, Clone)]
pub(crate) struct SharedProcessDataInner {
    pub(crate) signal_sender: SignalSender,
    pub(crate) signal_receiver: SignalReceiver,
    pub(crate) status_updater: StatusUpdater,
    pub(crate) status: StatusStream,
    pub(crate) pid: Pid,
}

impl Deref for SharedProcessData {
    type Target = SharedProcessDataInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Observable for SharedProcessData {
    async fn send_signal(&self, signal: SignalInterface) -> Result<(), SendError<SignalInterface>> {
        self.signal_sender.send(signal).await
    }
}

impl SharedProcessData {
    pub fn is_alive(&self) -> bool {
        self.status.is_alive()
    }

    pub fn pid(&self) -> &Pid {
        &self.pid
    }

    pub async fn watch_exit(&self) {
        self.status.clone().watch_exit().await
    }

    pub async fn watch_start(&self) {
        self.status.clone().watch_start().await
    }

    pub fn status_stream(&self) -> &StatusStream {
        &self.status
    }

    pub fn signal_receiver(&self) -> &SignalReceiver {
        &self.signal_receiver
    }

    pub fn new(pid: Pid) -> Self {
        let (signal_sender, signal_receiver) = SignalSender::new();
        let (status, status_updater) = StatusStream::new();

        Self {
            inner: Arc::new(SharedProcessDataInner {
                signal_sender,
                signal_receiver,
                status_updater,
                status,
                pid,
            }),
        }
    }
}
