use crate::*;
use polybox::{Payload, errors::SendError, oneshot::new_request};
use tokio::select;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Shutdown;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Kill;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Suspend;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Resume;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ActorStatus)]
pub struct GetStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
pub enum ActorStatus {
    Running,
    Suspended,
    ShuttingDown,
    Killed,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = DebugState)]
pub struct GetState;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DebugState {
    pub status: ActorStatus,
    #[schema(value_type = String, example = "2024-06-01T12:00:00Z")]
    pub uptime: std::time::Duration,
    pub description: String,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = "Vec<Pid>")]
pub struct GetChildren;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ())]
pub struct Ping;

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
pub enum SignalInterface {
    Shutdown(Payload<Shutdown>),
    Kill(Payload<Kill>),
    Suspend(Payload<Suspend>),
    Resume(Payload<Resume>),
    GetStatus(Payload<GetStatus>),
    GetState(Payload<GetState>),
    Ping(Payload<Ping>),
    GetChildren(Payload<GetChildren>),
}

impl SignalInterface {
    pub fn kind(&self) -> SignalKind {
        match self {
            SignalInterface::Shutdown(_) => SignalKind::Shutdown,
            SignalInterface::Kill(_) => SignalKind::Kill,
            SignalInterface::Suspend(_) => SignalKind::Suspend,
            SignalInterface::Resume(_) => SignalKind::Resume,
            SignalInterface::GetStatus(_) => SignalKind::GetStatus,
            SignalInterface::GetState(_) => SignalKind::GetState,
            SignalInterface::Ping(_) => SignalKind::Ping,
            SignalInterface::GetChildren(_) => SignalKind::GetChildren,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalKind {
    Shutdown,
    Kill,
    Suspend,
    Resume,
    GetStatus,
    GetState,
    Ping,
    GetChildren,
}

pub trait Observable {
    fn send_signal(
        &self,
        signal: SignalInterface,
    ) -> impl Future<Output = Result<(), SendError<SignalInterface>>> + Send;

    fn signal_shutdown(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal(&self, SignalInterface::Shutdown(Shutdown));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_exit(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal(&self, SignalInterface::Kill(Kill));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_suspend(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal(&self, SignalInterface::Suspend(Suspend));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_resume(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal(&self, SignalInterface::Resume(Resume));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn get_status(
        &self,
    ) -> impl Future<Output = Result<<GetStatus as Message>::Reply, RequestError<SignalInterface>>> + Send
    {
        let (tx, rx) = new_request();
        let fut = Self::send_signal(&self, SignalInterface::GetStatus((GetStatus, tx)));
        async {
            fut.await?;
            rx.await.map_err(Into::into)
        }
    }

    fn get_debug_state(
        &self,
    ) -> impl Future<Output = Result<<GetState as Message>::Reply, RequestError<SignalInterface>>> + Send
    {
        let (tx, rx) = new_request();
        let fut = Self::send_signal(&self, SignalInterface::GetState((GetState, tx)));
        async {
            fut.await?;
            rx.await.map_err(Into::into)
        }
    }

    fn ping(
        &self,
    ) -> impl Future<Output = Result<<Ping as Message>::Reply, RequestError<SignalInterface>>> + Send
    {
        let (tx, rx) = new_request();
        let fut = Self::send_signal(&self, SignalInterface::Ping((Ping, tx)));
        async {
            fut.await?;
            rx.await.map_err(Into::into)
        }
    }

    fn get_children(
        &self,
    ) -> impl Future<Output = Result<<GetChildren as Message>::Reply, RequestError<SignalInterface>>>
    + Send {
        let (tx, rx) = new_request();
        let fut = Self::send_signal(&self, SignalInterface::GetChildren((GetChildren, tx)));
        async {
            fut.await?;
            rx.await.map_err(Into::into)
        }
    }
}

#[derive(Clone, Debug)]
pub struct SignalSender {
    sender: async_channel::Sender<SignalInterface>,
}

impl SignalSender {
    pub(crate) fn new() -> (Self, SignalReceiver) {
        let (sender, receiver) = async_channel::bounded(1_000);
        (Self { sender }, SignalReceiver { receiver })
    }
}

impl SignalSender {
    pub async fn send(&self, signal: SignalInterface) -> Result<(), SendError<SignalInterface>> {
        self.sender.send(signal).await.map_err(|e| SendError(e.0))
    }
}

impl Observable for SignalSender {
    async fn send_signal(&self, signal: SignalInterface) -> Result<(), SendError<SignalInterface>> {
        self.sender.send(signal).await.map_err(|e| SendError(e.0))
    }
}

#[derive(Debug, Clone)]
pub struct SignalReceiver {
    receiver: async_channel::Receiver<SignalInterface>,
}

impl SignalReceiver {
    pub async fn recv(&mut self) -> Option<SignalInterface> {
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
pub enum Event<M> {
    Signal(SignalInterface),
    Message(M),
}

#[derive(Debug)]
pub enum SignalEvent {
    GetState(Tx<DebugState>),
    GetChildren(Tx<Vec<Pid>>),
    StatusUpdate(ActorStatus),
}

#[derive(Debug)]
pub enum ActorEvent<M> {
    Signal(SignalEvent),
    Message(M),
}
