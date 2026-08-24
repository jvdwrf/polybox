use crate::{
    handler::{Handler, HandlerInterface},
    *,
};
use rootcause::report;
use std::fmt::Debug;
use tokio::select;

pub struct HandlerState<H: Handler> {
    inbox: Inbox<H::Interface>,
}

impl<H: Handler> HandlerState<H> {
    pub fn new(inbox: Inbox<H::Interface>) -> Self {
        Self { inbox }
    }

    pub async fn exit(&mut self, handler: &mut H, reason: ExitReason) -> Result<(), Report> {
        handler.exit(reason).await
    }

    async fn init(&mut self, handler: &mut H) -> Result<(), InitError> {
        let address = self.address().clone();

        tokio::select! {
            res = handler.init(&address) => {
                res.map_err(InitError::Failed)
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
                Err(InitError::Cancelled)
            }
        }
    }

    pub async fn run(&mut self, handler: &mut H) -> Result<(), Report>
    where
        H: Handler + Debug,
    {
        if let Err(e) = self.init(handler).await {
            return self.exit(handler, e.into()).await;
        }

        loop {
            match self.run_once(handler).await {
                Ok(RunOnce::Continue) => {}

                Ok(RunOnce::ExitNormal) => {
                    break self.exit(handler, ExitReason::Normal).await;
                }

                Err(e) => {
                    break self.exit(handler, ExitReason::HandlerError(e)).await;
                }
            }
        }
    }

    async fn run_once(&mut self, handler: &mut H) -> Result<RunOnce, Report> {
        let msg = select! {
            msg = self.inbox.next() => {
                if let Some(msg) = msg {
                    msg
                } else {
                    return Ok(RunOnce::ExitNormal);
                }
            },

            scheduled = handler.schedule_next() => {
                let event = scheduled?;
                event.handle(self, handler).await?;
                return Ok(RunOnce::Continue);
            }
        };

        match msg {
            Event::Signal(signal) => match signal {
                Signal::Resume => {
                    handler.on_resume(self.address()).await?;
                    return Ok(RunOnce::Continue);
                }

                Signal::Suspend => {
                    handler.on_suspend(self.address()).await?;
                    return Ok(RunOnce::Continue);
                }

                Signal::Shutdown => {
                    handler.on_shutdown(self.address()).await?;
                    return Ok(RunOnce::Continue);
                }
            },

            Event::Message(msg) => {
                msg.handle_with(self, handler).await?;
                return Ok(RunOnce::Continue);
            }
        }
    }
}

impl<H: Handler> AsActorRef for HandlerState<H> {
    type ChannelSpec = H::Interface;

    fn as_channel(&self) -> &Channel<Self::ChannelSpec> {
        self.inbox.as_channel()
    }
}

enum InitError {
    Failed(Report),
    Cancelled,
}

impl From<InitError> for ExitReason {
    fn from(e: InitError) -> Self {
        match e {
            InitError::Failed(e) => ExitReason::InitFailed(e),
            InitError::Cancelled => ExitReason::InitCancelled,
        }
    }
}

enum RunOnce {
    Continue,
    ExitNormal,
}
