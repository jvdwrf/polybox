use crate::*;
use polybox::{Message, Payload, oneshot::new_request};

#[derive(Message)]
pub struct Shutdown;

#[derive(Message)]
pub struct Exit;

#[derive(Message)]
pub struct Suspend;

#[derive(Message)]
pub struct Resume;

#[derive(Message)]
#[msg(reply = Status)]
pub struct GetStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    Running,
    Suspended,
    Stopped,
}

#[derive(Message)]
#[msg(reply = State)]
pub struct GetState;

#[derive(Debug, Clone)]
pub struct State {
    pub status: Status,
    pub uptime: std::time::Duration,
    pub description: String,
    pub description_json: String,
}

#[derive(Message)]
pub struct Ping;

#[derive(Interface)]
pub enum Signal {
    Shutdown(Payload<Shutdown>),
    Exit(Payload<Exit>),
    Suspend(Payload<Suspend>),
    Resume(Payload<Resume>),
    GetStatus(Payload<GetStatus>),
    GetState(Payload<GetState>),
    Ping(Payload<Ping>),
}

impl Signal {
    pub fn kind(&self) -> SignalKind {
        match self {
            Signal::Shutdown(_) => SignalKind::Shutdown,
            Signal::Exit(_) => SignalKind::Exit,
            Signal::Suspend(_) => SignalKind::Suspend,
            Signal::Resume(_) => SignalKind::Resume,
            Signal::GetStatus(_) => SignalKind::GetStatus,
            Signal::GetState(_) => SignalKind::GetState,
            Signal::Ping(_) => SignalKind::Ping,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalKind {
    Shutdown,
    Exit,
    Suspend,
    Resume,
    GetStatus,
    GetState,
    Ping,
}

pub trait Observable {
    fn send_signal_payload(
        this: &Self,
        signal: Signal,
    ) -> impl Future<Output = Result<(), SendError<Signal>>> + Send;

    fn shutdown(&self) -> impl Future<Output = Result<Output<Shutdown>, SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Shutdown(Shutdown));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn exit(&self) -> impl Future<Output = Result<Output<Exit>, SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Exit(Exit));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn suspend(&self) -> impl Future<Output = Result<Output<Suspend>, SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Suspend(Suspend));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn resume(&self) -> impl Future<Output = Result<Output<Resume>, SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Resume(Resume));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn get_status(&self) -> impl Future<Output = Result<Output<GetStatus>, SendError<()>>> + Send {
        let (tx, rx) = new_request();
        let fut = Self::send_signal_payload(&self, Signal::GetStatus((GetStatus, tx)));
        async { fut.await.map_err(|_| SendError(())).map(|_| rx) }
    }

    fn get_state(&self) -> impl Future<Output = Result<Output<GetState>, SendError<()>>> + Send {
        let (tx, rx) = new_request();
        let fut = Self::send_signal_payload(&self, Signal::GetState((GetState, tx)));
        async { fut.await.map_err(|_| SendError(())).map(|_| rx) }
    }

    fn ping(&self) -> impl Future<Output = Result<Output<Ping>, SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Ping(Ping));
        async { fut.await.map_err(|_| SendError(())) }
    }
}

pub trait SendSignal<T: Message> {
    fn signal(&self, signal: T) -> impl Future<Output = Result<Output<T>, SendError<()>>> + Send;
}

impl<T: Observable> SendSignal<Shutdown> for T {
    fn signal(
        &self,
        _signal: Shutdown,
    ) -> impl Future<Output = Result<Output<Shutdown>, SendError<()>>> + Send {
        self.shutdown()
    }
}

impl<T: Observable> SendSignal<Exit> for T {
    fn signal(
        &self,
        _signal: Exit,
    ) -> impl Future<Output = Result<Output<Exit>, SendError<()>>> + Send {
        self.exit()
    }
}

impl<T: Observable> SendSignal<Suspend> for T {
    fn signal(
        &self,
        _signal: Suspend,
    ) -> impl Future<Output = Result<Output<Suspend>, SendError<()>>> + Send {
        self.suspend()
    }
}

impl<T: Observable> SendSignal<Resume> for T {
    fn signal(
        &self,
        _signal: Resume,
    ) -> impl Future<Output = Result<Output<Resume>, SendError<()>>> + Send {
        self.resume()
    }
}

impl<T: Observable> SendSignal<GetStatus> for T {
    fn signal(
        &self,
        _signal: GetStatus,
    ) -> impl Future<Output = Result<Output<GetStatus>, SendError<()>>> + Send {
        self.get_status()
    }
}

impl<T: Observable> SendSignal<GetState> for T {
    fn signal(
        &self,
        _signal: GetState,
    ) -> impl Future<Output = Result<Output<GetState>, SendError<()>>> + Send {
        self.get_state()
    }
}

impl<T: Observable> SendSignal<Ping> for T {
    fn signal(
        &self,
        _signal: Ping,
    ) -> impl Future<Output = Result<Output<Ping>, SendError<()>>> + Send {
        self.ping()
    }
}

#[derive(Clone, Debug)]
pub struct SignalSender {
    sender: tokio::sync::mpsc::Sender<Signal>,
}

#[derive(Debug)]
pub struct SignalReceiver {
    receiver: tokio::sync::mpsc::Receiver<Signal>,
}
