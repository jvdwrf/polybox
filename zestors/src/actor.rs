use crate::_prelude::*;
use polybox::{Interface, Message, Payload};
use std::fmt::{Debug, Display};

pub trait Actor: Debug + Sized + Send + 'static {
    type Interface: Interface + ActorInterface<Self>;
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
}

impl<T: Actor> Runnable for T {
    type Interface = T::Interface;
    type Exit = T::Exit;

    fn run(
        mut self,
        mut stream: EventStream<Self::Interface>,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static {
        async move {
            ActorState::new(address)
                .run(&mut self, &mut stream)
                .await
                .map_err(Into::into)
        }
    }
}

pub trait ActorInterface<T: Actor>: Interface {
    fn handle_with(
        self,
        state: &mut ActorState<T>,
        actor: &mut T,
    ) -> impl Future<Output = Result<(), T::Error>> + Send;
}

pub trait HandleMessage<T: Message>: Actor {
    fn handle_message(
        &mut self,
        state: &mut ActorState<Self>,
        msg: Payload<T>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
