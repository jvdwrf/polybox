use crate::{
    actor::{Actor, ActorExt as _},
    signals::{Observable, SignalOrMessage},
    *,
};
use polybox::errors::SendError;
use std::fmt::Debug;

pub struct ActorState<T: Actor> {
    status: signals::Status,
    start_time: tokio::time::Instant,
    address: Address<T::Interface>,
}

impl<T: Actor> ActorState<T> {
    pub fn new(address: Address<T::Interface>) -> Self {
        Self {
            status: signals::Status::Running,
            start_time: tokio::time::Instant::now(),
            address,
        }
    }

    pub fn address(&self) -> &Address<T::Interface> {
        &self.address
    }

    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    pub async fn run(
        &mut self,
        actor: &mut T,
        rx: &mut Receiver<T::Interface>,
        signal_rx: &mut SignalReceiver,
    ) -> Result<T::Exit, T::Error>
    where
        T: Actor + Debug,
    {
        loop {
            match self._run_once(actor, rx, signal_rx).await {
                Ok(None) => {}

                Ok(Some(exit)) => {
                    tracing::info!("Actor exited");
                    break Ok(exit);
                }

                Err(e) => {
                    tracing::warn!("Actor encountered an error: {e}. Attempting to recover...");

                    match actor.recover_error(e).await {
                        Ok(()) => {
                            tracing::info!("Actor recovered from error");
                        }
                        Err(e) => {
                            tracing::error!("Actor failed to recover from error: {e}");
                            break Err(e);
                        }
                    }
                }
            }
        }
    }

    async fn _run_once(
        &mut self,
        actor: &mut T,
        rx: &mut Receiver<T::Interface>,
        signal_rx: &mut SignalReceiver,
    ) -> Result<Option<T::Exit>, T::Error> {
        let Some(msg) = (match self.status {
            signals::Status::Running | signals::Status::Exiting => signal_rx.recv_with(rx).await,
            signals::Status::Suspended => signal_rx.recv().await.map(SignalOrMessage::Signal),
        }) else {
            return actor.exit(ExitReason::Shutdown).await.map(Some);
        };

        match msg {
            SignalOrMessage::Signal(signal) => match signal {
                Signal::Shutdown(_) => {
                    self.status = signals::Status::Exiting;
                    actor.on_shutdown().await?;
                }

                Signal::Kill(_) => {
                    self.status = signals::Status::Exiting;
                    actor.on_kill().await?;
                    return actor.exit(ExitReason::Kill).await.map(Some);
                }

                Signal::Suspend(_) => {
                    if self.status == signals::Status::Exiting {
                        tracing::warn!("Actor is exiting, cannot suspend");
                        return Ok(None);
                    }

                    actor.on_suspend().await?;
                    self.status = signals::Status::Suspended;
                }

                Signal::Resume(_) => {
                    if self.status != signals::Status::Suspended {
                        tracing::warn!("Actor is not suspended, cannot resume");
                        return Ok(None);
                    }

                    actor.on_resume().await?;
                    self.status = signals::Status::Running;
                }

                Signal::GetStatus((_, tx)) => {
                    let _ = tx.send(self.status);
                }

                Signal::GetState((_, tx)) => {
                    let _ = tx.send(signals::State {
                        status: self.status,
                        uptime: self.uptime(),
                        description: actor.debug_state(),
                    });
                }

                Signal::Ping((_, tx)) => {
                    let _ = tx.send(());
                }
            },

            SignalOrMessage::Message(msg) => {
                actor.handle_interface(self, msg).await?;
            }
        }

        Ok(None)
    }
}

impl<T: Actor> Observable for ActorState<T> {
    async fn send_signal_payload(this: &Self, signal: Signal) -> Result<(), SendError<Signal>> {
        <Address<T::Interface> as Observable>::send_signal_payload(&this.address, signal).await
    }
}

impl<T: Actor, M: Message> Sends<M> for ActorState<T>
where
    Address<T::Interface>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<Output<M>, SendError<M>> {
        self.address.send(msg).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExitReason {
    Shutdown,
    Kill,
    UnhandledError,
}
