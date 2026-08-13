use crate::{
    handler::{Handler, HandlerInterface},
    signals::{Event, Observable},
    *,
};
use polybox::errors::SendError;
use std::fmt::Debug;
use tokio::select;

pub struct HandlerState<H: Handler> {
    status: signals::Status,
    start_time: tokio::time::Instant,
    address: Address<H::Interface>,
}

impl<H: Handler> HandlerState<H> {
    pub fn new(address: Address<H::Interface>) -> Self {
        Self {
            status: signals::Status::Running,
            start_time: tokio::time::Instant::now(),
            address,
        }
    }

    pub fn address(&self) -> &Address<H::Interface> {
        &self.address
    }

    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    pub async fn run(
        &mut self,
        handler: &mut H,
        stream: &mut EventStream<H::Interface>,
    ) -> Result<H::Exit, H::Error>
    where
        H: Handler + Debug,
    {
        loop {
            match self._run_once(handler, stream).await {
                Ok(None) => {}

                Ok(Some(exit)) => {
                    tracing::info!("Actor exited");
                    break Ok(exit);
                }

                Err(e) => {
                    tracing::warn!("Actor encountered an error: {e}. Attempting to recover...");

                    match handler.recover_error(e).await {
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
        handler: &mut H,
        stream: &mut EventStream<H::Interface>,
    ) -> Result<Option<H::Exit>, H::Error> {
        let enable_messages = match self.status {
            signals::Status::Running | signals::Status::Exiting => true,
            signals::Status::Suspended => false,
        };

        let msg = select! {
            Some(msg) = stream.recv_enabled(enable_messages) => msg,

            next = handler.schedule_next() => {
                match next {
                    Ok(event) => {
                        event.handle(self, handler).await?;
                        return Ok(None);
                    }
                    Err(e) => {
                        tracing::error!("Actor encountered an error: {e}");
                        return Err(e);
                    }
                }
            }

            else => return handler.exit(ExitReason::Shutdown).await.map(Some),
        };

        match msg {
            Event::Signal(signal) => match signal {
                Signal::Shutdown(_) => {
                    self.status = signals::Status::Exiting;
                    handler.on_shutdown().await?;
                }

                Signal::Exit(_) => {
                    self.status = signals::Status::Exiting;
                    handler.on_kill().await?;
                    return handler.exit(ExitReason::Kill).await.map(Some);
                }

                Signal::Suspend(_) => {
                    if self.status == signals::Status::Exiting {
                        tracing::warn!("Actor is exiting, cannot suspend");
                        return Ok(None);
                    }

                    handler.on_suspend().await?;
                    self.status = signals::Status::Suspended;
                }

                Signal::Resume(_) => {
                    if self.status != signals::Status::Suspended {
                        tracing::warn!("Actor is not suspended, cannot resume");
                        return Ok(None);
                    }

                    handler.on_resume().await?;
                    self.status = signals::Status::Running;
                }

                Signal::GetStatus((_, tx)) => {
                    tx.send(self.status).ok();
                }

                Signal::GetState((_, tx)) => {
                    let _ = tx.send(signals::DebugState {
                        status: self.status,
                        uptime: self.uptime(),
                        description: handler.debug_state(),
                    });
                }

                Signal::Ping((_, tx)) => {
                    tx.send(()).ok();
                }
                Signal::GetChildren((_, tx)) => {
                    tx.send(handler.children()).ok();
                }
            },

            Event::Message(msg) => {
                msg.handle_with(self, handler).await?;
            }
        }

        Ok(None)
    }
}

impl<H: Handler> Observable for HandlerState<H> {
    async fn send_signal(&self, signal: Signal) -> Result<(), SendError<Signal>> {
        <Address<H::Interface> as Observable>::send_signal(&self.address, signal).await
    }
}

impl<H: Handler, M: Message> Sends<M> for HandlerState<H>
where
    Address<H::Interface>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
        self.address.send(msg).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ExitReason {
    Shutdown,
    Kill,
    UnhandledError,
}
