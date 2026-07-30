use std::fmt::Debug;

use crate::{actor::Actor, signals::SignalOrMessage, *};

pub struct ActorState {
    pub status: signals::Status,
    pub start_time: tokio::time::Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExitReason {
    Shutdown,
    Kill,
    UnhandledError,
}

impl ActorState {
    pub fn new() -> Self {
        Self {
            status: signals::Status::Running,
            start_time: tokio::time::Instant::now(),
        }
    }

    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    pub async fn run<T>(
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

    async fn _run_once<T>(
        &mut self,
        actor: &mut T,
        rx: &mut Receiver<T::Interface>,
        signal_rx: &mut SignalReceiver,
    ) -> Result<Option<T::Exit>, T::Error>
    where
        T: Actor + Debug,
    {
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
                    actor.on_suspend().await?;
                    self.status = signals::Status::Suspended;
                }

                Signal::Resume(_) => {
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
                actor.handle_message(msg).await?;
            }
        }

        Ok(None)
    }
}
