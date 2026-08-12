use crate::*;
use polybox::{Payload, errors::SendError, oneshot::new_request};
use tokio::select;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Shutdown;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Exit;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Suspend;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Resume;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = Status)]
pub struct GetStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    Running,
    Suspended,
    Exiting,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = State)]
pub struct GetState;

#[derive(Debug, Clone)]
pub struct State {
    pub status: Status,
    pub uptime: std::time::Duration,
    pub description: String,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ())]
pub struct Ping;

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
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

    fn signal_shutdown(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Shutdown(Shutdown));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_exit(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Exit(Exit));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_suspend(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Suspend(Suspend));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_resume(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal_payload(&self, Signal::Resume(Resume));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_get_status(
        &self,
    ) -> impl Future<Output = Result<<GetStatus as Message>::Output, SendError<()>>> + Send {
        let (tx, rx) = new_request();
        let fut = Self::send_signal_payload(&self, Signal::GetStatus((GetStatus, tx)));
        async { fut.await.map_err(|_| SendError(())).map(|_| rx) }
    }

    fn signal_get_state(
        &self,
    ) -> impl Future<Output = Result<<GetState as Message>::Output, SendError<()>>> + Send {
        let (tx, rx) = new_request();
        let fut = Self::send_signal_payload(&self, Signal::GetState((GetState, tx)));
        async { fut.await.map_err(|_| SendError(())).map(|_| rx) }
    }

    fn signal_ping(
        &self,
    ) -> impl Future<Output = Result<<Ping as Message>::Output, SendError<()>>> + Send {
        let (tx, rx) = new_request();
        let fut = Self::send_signal_payload(&self, Signal::Ping((Ping, tx)));
        async { fut.await.map_err(|_| SendError(())).map(|_| rx) }
    }
}

pub trait SendSignal<T: Message> {
    fn signal(
        &self,
        signal: T,
    ) -> impl Future<Output = Result<<T as Message>::Output, SendError<()>>> + Send;
}

impl<T: Observable> SendSignal<Shutdown> for T {
    fn signal(
        &self,
        _signal: Shutdown,
    ) -> impl Future<Output = Result<<Shutdown as Message>::Output, SendError<()>>> + Send {
        self.signal_shutdown()
    }
}

impl<T: Observable> SendSignal<Exit> for T {
    fn signal(
        &self,
        _signal: Exit,
    ) -> impl Future<Output = Result<<Exit as Message>::Output, SendError<()>>> + Send {
        self.signal_exit()
    }
}

impl<T: Observable> SendSignal<Suspend> for T {
    fn signal(
        &self,
        _signal: Suspend,
    ) -> impl Future<Output = Result<<Suspend as Message>::Output, SendError<()>>> + Send {
        self.signal_suspend()
    }
}

impl<T: Observable> SendSignal<Resume> for T {
    fn signal(
        &self,
        _signal: Resume,
    ) -> impl Future<Output = Result<<Resume as Message>::Output, SendError<()>>> + Send {
        self.signal_resume()
    }
}

impl<T: Observable> SendSignal<GetStatus> for T {
    fn signal(
        &self,
        _signal: GetStatus,
    ) -> impl Future<Output = Result<<GetStatus as Message>::Output, SendError<()>>> + Send {
        self.signal_get_status()
    }
}

impl<T: Observable> SendSignal<GetState> for T {
    fn signal(
        &self,
        _signal: GetState,
    ) -> impl Future<Output = Result<<GetState as Message>::Output, SendError<()>>> + Send {
        self.signal_get_state()
    }
}

impl<T: Observable> SendSignal<Ping> for T {
    fn signal(
        &self,
        _signal: Ping,
    ) -> impl Future<Output = Result<<Ping as Message>::Output, SendError<()>>> + Send {
        self.signal_ping()
    }
}

#[derive(Clone, Debug)]
pub struct SignalSender {
    sender: async_channel::Sender<Signal>,
}

impl SignalSender {
    pub(crate) fn new() -> (Self, SignalReceiver) {
        let (sender, receiver) = async_channel::bounded(1_000);
        (Self { sender }, SignalReceiver { receiver })
    }
}

impl SignalSender {
    pub async fn send(&self, signal: Signal) -> Result<(), SendError<Signal>> {
        self.sender.send(signal).await.map_err(|e| SendError(e.0))
    }
}

impl Observable for SignalSender {
    async fn send_signal_payload(this: &Self, signal: Signal) -> Result<(), SendError<Signal>> {
        this.sender.send(signal).await.map_err(|e| SendError(e.0))
    }
}

#[derive(Debug, Clone)]
pub struct SignalReceiver {
    receiver: async_channel::Receiver<Signal>,
}

impl SignalReceiver {
    pub async fn recv(&mut self) -> Option<Signal> {
        self.receiver.recv().await.ok()
    }

    pub async fn recv_with<T>(&mut self, other: &mut Receiver<T>) -> Option<Event<T>> {
        select! {
            biased;

            Ok(signal) = self.receiver.recv() => {
                Some(Event::Signal(signal))
            }

            Some(msg) = other.recv() => {
                Some(Event::Message(msg))
            }

            else => None,
        }
    }

    pub async fn recv_with_enabled<T>(
        &mut self,
        other: &mut Receiver<T>,
        enabled: bool,
    ) -> Option<Event<T>> {
        if enabled {
            select! {
                biased;

                Ok(signal) = self.receiver.recv() => {
                    Some(Event::Signal(signal))
                }

                Some(msg) = other.recv() => {
                    Some(Event::Message(msg))
                }

                else => None,
            }
        } else {
            if let Ok(signal) = self.receiver.recv().await {
                Some(Event::Signal(signal))
            } else {
                None
            }
        }
    }
}

#[derive(Debug)]
pub enum Event<T> {
    Signal(Signal),
    Message(T),
}
