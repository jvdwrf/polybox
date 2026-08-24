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

    pub async fn exit_actor(
        &mut self,
        handler: &mut H,
        error: Option<Report>,
    ) -> Result<(), Report> {
        tracing::info!("Actor is exiting due to reason");
        let address = self.address().clone();

        let res = match error {
            Some(err) => Err(err),
            None => Ok(()),
        };

        tokio::select! {
            res = handler.exit(res, &address) => {
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

    pub async fn run(&mut self, handler: &mut H) -> Result<(), Report>
    where
        H: Handler + Debug,
    {
        let address = self.address().clone();

        tokio::select! {
            res = handler.init(&address) => {
                match res {
                    Ok(_) => {
                        tracing::debug!("Handler initialized successfully");
                    }
                    Err(e) => {
                        tracing::warn!("Handler failed to initialize");
                        return self.exit_actor(handler, Some(e)).await;
                    }
                }
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
                    .exit_actor(handler, None)
                    .await
            }
        }

        loop {
            match self._run_once(handler).await {
                Ok(RunOnce::Continue) => {
                    tracing::trace!("Actor loop iteration completed, continuing...");
                }

                Ok(RunOnce::Finished) => {
                    tracing::debug!("Handler exited");
                    break self.exit_actor(handler, None).await;
                }

                Err(e) => {
                    tracing::warn!("Handler encountered an error. Attempting to recover...");
                    break self.exit_actor(handler, Some(e)).await;
                }
            }
        }
    }

    async fn _run_once(&mut self, handler: &mut H) -> Result<RunOnce, Report> {
        let stream = &mut self.inbox;

        if stream.status().should_exit() && stream.is_empty() {
            tracing::debug!("Actor is exiting due to status: {:?}", stream.status());
            return Ok(RunOnce::Finished);
        }

        let msg = select! {
            Some(msg) = stream.next() => msg,

            next = handler.schedule_next() => {
                match next {
                    Ok(event) => {
                        event.handle(self, handler).await?;
                        return Ok(RunOnce::Continue);
                    }
                    Err(e) => {
                        tracing::error!("Handler encountered an error");
                        return Err(e);
                    }
                }
            }

            else => return Ok(RunOnce::Finished),
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

        Ok(RunOnce::Continue)
    }
}

enum RunOnce {
    Continue,
    Finished,
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
