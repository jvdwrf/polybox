use crate::{
    handler::{Handler, HandlerInterface},
    *,
};
use std::fmt::Debug;
use tokio::select;

pub struct HandlerState<H: Handler> {
    inbox: Inbox<H::Interface>,
}

impl<H: Handler> HandlerState<H> {
    pub fn new(inbox: Inbox<H::Interface>) -> Self {
        Self { inbox }
    }

    pub async fn exit_while_receiving_signals(
        &mut self,
        handler: &mut H,
        reason: HandlerShutdownReason,
    ) -> Result<H::Exit, H::Error> {
        tracing::info!("Actor is exiting due to reason: {:?}", reason);
        let address = self.address().clone();

        tokio::select! {
            res = handler.shut_down(&address) => {
                res
            }

            _ = async {
                while let Some(signal) = self.inbox.next_signal().await {
                    match signal {
                        Signal::Resume | Signal::Suspend | Signal::Shutdown => {
                            tracing::debug!("Ignoring signal {:?} while exiting", signal);
                        }
                    }
                }
                futures::future::pending::<()>().await
            } => {
                unreachable!("");
            }
        }
    }

    pub async fn run(&mut self, handler: &mut H) -> Result<H::Exit, H::Error>
    where
        H: Handler + Debug,
    {
        let address = self.address().clone();

        tokio::select! {
            res = handler.init(&address) => {
                res?;
            }

            _shutdown_signal_received = async {
                while let Some(signal) = self.inbox.next_signal().await {
                    match signal {
                        Signal::Shutdown => {
                            break;
                        }
                        Signal::Resume | Signal::Suspend => {
                            tracing::debug!("Ignoring signal {:?} while initializing", signal);
                        }
                    }
                }
            } => {
                tracing::debug!("Actor is exiting due to shutdown signal");
                return self
                    .exit_while_receiving_signals(handler, HandlerShutdownReason::Shutdown)
                    .await
            }
        }

        loop {
            match self._run_once(handler).await {
                Ok(None) => {
                    tracing::trace!("Actor loop iteration completed, continuing...");
                }

                Ok(Some(exit)) => {
                    tracing::debug!("Handler exited");
                    break Ok(exit);
                }

                Err(e) => {
                    tracing::warn!("Handler encountered an error. Attempting to recover...");

                    match handler.recover_error(self.address(), e).await {
                        Ok(()) => {
                            tracing::info!("Handler recovered from error");
                        }
                        Err(e) => {
                            tracing::error!("Handler failed to recover from error");
                            break Err(e);
                        }
                    }
                }
            }
        }
    }

    async fn _run_once(&mut self, handler: &mut H) -> Result<Option<H::Exit>, H::Error> {
        let stream = &mut self.inbox;

        if stream.status().should_exit() && stream.is_empty() {
            tracing::debug!("Actor is exiting due to status: {:?}", stream.status());
            return self
                .exit_while_receiving_signals(handler, HandlerShutdownReason::Shutdown)
                .await
                .map(Some);
        }

        let msg = select! {
            Some(msg) = stream.next() => msg,

            next = handler.schedule_next() => {
                match next {
                    Ok(event) => {
                        event.handle(self, handler).await?;
                        return Ok(None);
                    }
                    Err(e) => {
                        tracing::error!("Handler encountered an error");
                        return Err(e);
                    }
                }
            }

            else => return self
                .exit_while_receiving_signals(handler, HandlerShutdownReason::Shutdown)
                .await
                .map(Some),
        };

        match msg {
            Event::Signal(signal) => match signal {
                Signal::Resume => {
                    handler.on_resume(self.address()).await?;
                }

                Signal::Suspend => {
                    handler.on_suspend(self.address()).await?;
                }

                Signal::Shutdown => {
                    handler.on_shutdown(self.address()).await?;
                }
            },

            Event::Message(msg) => {
                msg.handle_with(self, handler).await?;
            }
        }

        Ok(None)
    }
}

impl<H: Handler> AsActorRef for HandlerState<H> {
    type ChannelSpec = H::Interface;

    fn as_channel(&self) -> &Channel<Self::ChannelSpec> {
        self.inbox.as_channel()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HandlerShutdownReason {
    Shutdown,
    UnhandledError,
}
