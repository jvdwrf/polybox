use crate::_prelude::*;
use futures::future::pending;
use smol_str::format_smolstr;
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
};

pub trait Handler: Debug + Sized + Send + Sync + 'static {
    type Interface: Interface + HandlerInterface<Self>;
    type Error: Debug + Display + Send + 'static + Into<Report>;
    type Exit: Send + 'static;

    fn init(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Called when the actor is exiting, after all messages have been processed.
    fn exit(
        &mut self,
        _address: &Address<Self::Interface>,
        reason: ShutdownReason,
    ) -> impl Future<Output = Result<Self::Exit, Self::Error>> + Send;

    /// Called when the actor encounters an error.
    ///
    /// Returning `Ok(())` will allow the actor to continue running, while returning `Err(e)` will cause the actor to exit with the error `e`.
    fn recover_error(
        &mut self,
        _address: &Address<Self::Interface>,
        error: Self::Error,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Err(error) }
    }

    /// Called when the actor starts shutting down.
    ///
    /// After this method is called, the actor will stop receiving messages, but
    /// will still empty its message queue before exiting.
    ///
    /// The actor will exit using [`ExitReason::Shutdown`].
    fn on_shutdown(
        &mut self,
        _address: &Address<Self::Interface>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    fn on_suspend(
        &mut self,
        _address: &Address<Self::Interface>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    fn on_resume(
        &mut self,
        _address: &Address<Self::Interface>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    fn debug_state(&self, _address: &Address<Self::Interface>) -> DebugState {
        format_smolstr!("{self:?}").into()
    }

    /// Called whenever the actor is waiting for a new event to process.
    ///
    /// When this returns a value, the actor will then call [`Handle::handle`]
    fn schedule_next(
        &mut self,
    ) -> impl Future<Output = Result<impl HandledBy<Self>, Self::Error>> + Send {
        pending::<Result<Infallible, _>>()
    }

    fn children(&self) -> Vec<ChildDescription> {
        vec![]
    }
}

impl<H: Handler> Actor for H {
    type Interface = H::Interface;
    type Exit = H::Exit;

    async fn run(mut self, state: EventStream<Self::Interface>) -> Result<Self::Exit, Report> {
        HandlerState::new(state)
            .run(&mut self)
            .await
            .map_err(Into::into)
    }
}

pub trait HandlerInterface<H: Handler>: Interface {
    fn handle_with(
        self,
        state: &mut HandlerState<H>,
        actor: &mut H,
    ) -> impl Future<Output = Result<(), H::Error>> + Send;
}

pub trait Handle<M: Message>: Handler {
    fn handle(
        &mut self,
        state: &mut HandlerState<Self>,
        msg: Payload<M>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<H: Handler> Handle<Infallible> for H {
    fn handle(
        &mut self,
        _state: &mut HandlerState<Self>,
        _msg: Payload<Infallible>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { unreachable!("Infallible message should never be sent") }
    }
}

pub trait HandledBy<H: Handler>: Message<Payload = Self> {
    fn handle(
        self,
        state: &mut HandlerState<H>,
        actor: &mut H,
    ) -> impl Future<Output = Result<(), H::Error>> + Send;
}

impl<H, M> HandledBy<H> for M
where
    H: Handle<M>,
    M: Message<Payload = Self>,
{
    fn handle(
        self,
        state: &mut HandlerState<H>,
        actor: &mut H,
    ) -> impl Future<Output = Result<(), H::Error>> + Send {
        actor.handle(state, self)
    }
}
