use crate::_prelude::*;
use futures::future::pending;
use polybox::{Interface, Message, Payload};
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
};

pub trait Handler: Debug + Sized + Send + Sync + 'static {
    type Interface: Interface + HandlerInterface<Self>;
    type Error: Debug + Display + Send + 'static + Into<anyhow::Error>;
    type Exit: Send + 'static;

    /// Called when the actor is exiting, after all messages have been processed.
    fn exit(
        &mut self,
        reason: ExitReason,
    ) -> impl Future<Output = Result<Self::Exit, Self::Error>> + Send + '_;

    /// Called when the actor encounters an error.
    ///
    /// Returning `Ok(())` will allow the actor to continue running, while returning `Err(e)` will cause the actor to exit with the error `e`.
    fn recover_error(
        &mut self,
        error: Self::Error,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        async { Err(error) }
    }

    /// Called when the actor starts shutting down.
    ///
    /// After this method is called, the actor will stop receiving messages, but
    /// will still empty its message queue before exiting.
    ///
    /// The actor will exit using [`ExitReason::Shutdown`].
    fn on_shutdown(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        async { Ok(()) }
    }

    /// Called when the actor is killed.
    ///
    /// No more messages will be processed after this method is called.
    ///
    /// The actor will exit using [`ExitReason::Kill`].
    fn on_kill(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        async { Ok(()) }
    }

    fn on_suspend(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        async { Ok(()) }
    }

    fn on_resume(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        async { Ok(()) }
    }

    fn debug_state(&self) -> String {
        format!("{self:?}")
    }

    /// Called whenever the actor is waiting for a new event to process.
    ///
    /// When this returns a value, the actor will then call [`Handle::handle`]
    fn schedule_next(
        &mut self,
    ) -> impl Future<Output = Result<impl HandledBy<Self>, Self::Error>> + Send + '_ {
        pending::<Result<Infallible, _>>()
    }
}

impl<T: Handler> Actor for T {
    type Interface = T::Interface;
    type Exit = T::Exit;

    fn run(
        mut self,
        mut stream: EventStream<Self::Interface>,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static {
        async move {
            HandlerState::new(address)
                .run(&mut self, &mut stream)
                .await
                .map_err(Into::into)
        }
    }
}

pub trait HandlerInterface<T: Handler>: Interface {
    fn handle_with(
        self,
        state: &mut HandlerState<T>,
        actor: &mut T,
    ) -> impl Future<Output = Result<(), T::Error>> + Send;
}

pub trait Handle<T: Message>: Handler {
    fn handle(
        &mut self,
        state: &mut HandlerState<Self>,
        msg: Payload<T>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<T: Handler> Handle<Infallible> for T {
    fn handle(
        &mut self,
        _state: &mut HandlerState<Self>,
        _msg: Payload<Infallible>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
}

pub trait HandledBy<T: Handler>: Message<Kind: MessageSpecifier<Self, Payload = Self>> {
    fn handle(
        self,
        state: &mut HandlerState<T>,
        actor: &mut T,
    ) -> impl Future<Output = Result<(), T::Error>> + Send;
}

impl<H, T> HandledBy<H> for T
where
    H: Handle<T>,
    T: Message<Kind: MessageSpecifier<Self, Payload = Self>>,
{
    fn handle(
        self,
        state: &mut HandlerState<H>,
        actor: &mut H,
    ) -> impl Future<Output = Result<(), H::Error>> + Send {
        actor.handle(state, self)
    }
}
