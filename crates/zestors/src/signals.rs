use crate::{schemas::DurationSchema, *};
use polybox::{Payload, errors::SendError, oneshot::new_request};
use tokio::{select, time::Instant};

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
}

impl ActorStatus {
    pub fn should_exit(&self) -> bool {
        matches!(self, ActorStatus::ShuttingDown)
    }

    pub fn should_accept_messages(&self) -> bool {
        !matches!(self, ActorStatus::Suspended)
    }

    pub fn running(&self) -> bool {
        matches!(self, ActorStatus::Running)
    }

    pub fn suspended(&self) -> bool {
        matches!(self, ActorStatus::Suspended)
    }

    pub fn shutting_down(&self) -> bool {
        matches!(self, ActorStatus::ShuttingDown)
    }
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = DebugState)]
pub struct GetState;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DebugState {
    pub status: ActorStatus,

    #[schema(value_type = DurationSchema)]
    #[serde(with = "DurationSchema")]
    pub uptime: std::time::Duration,

    pub description: String,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = "Vec<ChildDescription>")]
pub struct GetChildren;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildDescription {
    pub pid: Pid,
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ())]
pub struct Ping;

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
pub enum SignalInterface {
    Shutdown(Payload<Shutdown>),
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
        self.sender.send(signal).await.map_err(|e| {
            tracing::debug!(?e, "Failed to send signal");
            SendError(e.0)
        })
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

    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
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
    GetChildren(Tx<Vec<ChildDescription>>),
    StatusUpdate(ActorStatus),
}

#[derive(Debug)]
pub enum ActorEvent<M> {
    Signal(SignalEvent),
    Message(M),
}
