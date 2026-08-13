use crate::{
    handler::{Handler, HandlerInterface},
    signals::Observable,
    *,
};
use polybox::errors::SendError;
use std::fmt::Debug;
use tokio::select;

pub struct HandlerState<H: Handler> {
    inner: ActorState<H::Interface>,
}

impl<H: Handler> HandlerState<H> {
    pub fn new(inner: ActorState<H::Interface>) -> Self {
        Self { inner }
    }

    pub fn address(&self) -> &Address<H::Interface> {
        &self.inner.address()
    }

    pub fn uptime(&self) -> std::time::Duration {
        self.inner.uptime()
    }

    pub async fn run(&mut self, handler: &mut H) -> Result<H::Exit, H::Error>
    where
        H: Handler + Debug,
    {
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
                    tracing::warn!("Handler encountered an error: {e}. Attempting to recover...");

                    match handler.recover_error(e).await {
                        Ok(()) => {
                            tracing::info!("Handler recovered from error");
                        }
                        Err(e) => {
                            tracing::error!("Handler failed to recover from error: {e}");
                            break Err(e);
                        }
                    }
                }
            }
        }
    }

    async fn _run_once(&mut self, handler: &mut H) -> Result<Option<H::Exit>, H::Error> {
        let state = &mut self.inner;

        if state.status().should_exit() && state.is_empty() {
            tracing::debug!("Actor is exiting due to status: {:?}", state.status());
            return handler.exit(ExitReason::Shutdown).await.map(Some);
        }

        let msg = select! {
            Some(msg) = state.next() => msg,

            next = handler.schedule_next() => {
                match next {
                    Ok(event) => {
                        event.handle(self, handler).await?;
                        return Ok(None);
                    }
                    Err(e) => {
                        tracing::error!("Handler encountered an error: {e}");
                        return Err(e);
                    }
                }
            }

            else => return handler.exit(ExitReason::Shutdown).await.map(Some),
        };

        match msg {
            ActorEvent::Signal(signal) => match signal {
                SignalEvent::StatusUpdate(status) => match status {
                    ActorStatus::Running => {
                        handler.on_resume().await?;
                    }
                    ActorStatus::Suspended => {
                        handler.on_suspend().await?;
                    }
                    ActorStatus::ShuttingDown => {
                        handler.on_shutdown().await?;
                    }
                },

                SignalEvent::GetState(tx) => {
                    let _ = tx.send(signals::DebugState {
                        status: state.status(),
                        uptime: state.uptime(),
                        description: handler.debug_state(),
                    });
                }

                SignalEvent::GetChildren(tx) => {
                    tx.send(handler.children()).ok();
                }
            },

            ActorEvent::Message(msg) => {
                msg.handle_with(self, handler).await?;
            }
        }

        Ok(None)
    }
}

impl<H: Handler> Observable for HandlerState<H> {
    async fn send_signal(&self, signal: SignalInterface) -> Result<(), SendError<SignalInterface>> {
        <Address<H::Interface> as Observable>::send_signal(&self.address(), signal).await
    }
}

impl<H: Handler, M: Message> Sends<M> for HandlerState<H>
where
    Address<H::Interface>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
        self.address().send(msg).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ExitReason {
    Shutdown,
    UnhandledError,
}
