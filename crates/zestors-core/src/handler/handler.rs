use crate::_prelude::*;
use futures::future::pending;
use rootcause::report;
use std::{convert::Infallible, fmt::Debug};

#[derive(Debug)]
pub enum ExitReason {
    Normal,
    InitFailed(Report),
    InitCancelled,
    HandlerError(Report),
}

impl ExitReason {
    pub fn into_result(self) -> Result<(), Report> {
        match self {
            ExitReason::Normal => Ok(()),
            ExitReason::InitFailed(e) => Err(e.attach("Handler initialization failed")),
            ExitReason::InitCancelled => Err(report!("Handler initialization cancelled")),
            ExitReason::HandlerError(e) => Err(e.attach("Handler encountered an error")),
        }
    }
}

impl From<ExitReason> for Result<(), Report> {
    fn from(reason: ExitReason) -> Self {
        reason.into_result()
    }
}

pub trait Handler: Debug + Sized + Send + 'static {
    type Interface: HandlerInterface<Self>;

    /// Called when the actor is first spawned, before any messages are processed.
    /// This corresponds to [`ActorStatus::Initializing`].
    fn init(
        &mut self,
        _address: &Address<Self::Interface>,
    ) -> impl Future<Output = Result<(), Report>> + Send {
        async { Ok(()) }
    }

    /// Called when the actor is shutting down, after all messages have been processed.
    /// This corresponds to [`ActorStatus::ShuttingDown`].
    fn exit(&mut self, reason: ExitReason) -> impl Future<Output = Result<(), Report>> + Send {
        async { reason.into_result() }
    }

    /// Called when the actor receives [`Signal::Shutdown`].
    ///
    /// The `Shutdown` signal automatically prevents the actor from receiving any
    /// new messages, and will cause the actor to exit after all messages have
    /// been processed. This method is only for performing any additional actions
    /// that may be needed when the actor is shutting down.
    fn on_shutdown(
        &mut self,
        _address: &Address<Self::Interface>,
    ) -> impl Future<Output = Result<(), Report>> + Send {
        async { Ok(()) }
    }

    /// Called when the actor receives [`Signal::Suspend`].
    ///
    /// The `Suspend` signal automatically pauses the actor's message processing.
    /// This method is only for performing any additional actions that may be needed when the actor is suspended.
    fn on_suspend(
        &mut self,
        _address: &Address<Self::Interface>,
    ) -> impl Future<Output = Result<(), Report>> + Send {
        async { Ok(()) }
    }

    /// Called when the actor receives [`Signal::Resume`].
    ///
    /// The `Resume` signal automatically resumes the actor's message processing.
    /// This method is only for performing any additional actions that may be needed when the actor is resumed.
    fn on_resume(
        &mut self,
        _address: &Address<Self::Interface>,
    ) -> impl Future<Output = Result<(), Report>> + Send {
        async { Ok(()) }
    }

    /// Called whenever the actor is waiting for a new event to process.
    ///
    /// When this returns a value, the actor will then call [`Handle::handle`]
    fn schedule_next(
        &mut self,
    ) -> impl Future<Output = Result<impl HandledBy<Self>, Report>> + Send {
        pending::<Result<Infallible, _>>()
    }

    fn children(&self) -> Vec<ChildDescription> {
        vec![]
    }
}

impl<H: Handler> Actor for H {
    type Interface = H::Interface;
    type Exit = ();

    async fn run(mut self, state: Inbox<Self::Interface>) -> Result<Self::Exit, Report> {
        HandlerState::new(state).run(&mut self).await
    }
}

pub trait HandlerInterface<H: Handler>: Interface {
    fn handle_with(
        self,
        state: &mut HandlerState<H>,
        actor: &mut H,
    ) -> impl Future<Output = Result<(), Report>> + Send;
}

pub trait Handle<M: Message>: Handler {
    fn handle(
        &mut self,
        state: &mut HandlerState<Self>,
        msg: Envelope<M>,
    ) -> impl Future<Output = Result<(), Report>> + Send;
}

impl<H: Handler> Handle<Infallible> for H {
    fn handle(
        &mut self,
        _state: &mut HandlerState<Self>,
        _msg: Envelope<Infallible>,
    ) -> impl Future<Output = Result<(), Report>> + Send {
        async { unreachable!("Infallible message should never be sent") }
    }
}

pub trait HandledBy<H: Handler>: Message<Mode = FireAndForget, Outcome = ()> {
    fn handle(
        self,
        state: &mut HandlerState<H>,
        actor: &mut H,
    ) -> impl Future<Output = Result<(), Report>> + Send;
}

impl<H, M> HandledBy<H> for M
where
    H: Handle<M>,
    M: Message<Mode = FireAndForget, Outcome = ()>,
{
    fn handle(
        self,
        state: &mut HandlerState<H>,
        actor: &mut H,
    ) -> impl Future<Output = Result<(), Report>> + Send {
        actor.handle(state, Envelope::new(self, ()))
    }
}
