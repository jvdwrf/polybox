use crate::_prelude::*;
use futures::{FutureExt, Stream, StreamExt as _};
use indexmap::IndexMap;
use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::Instant;

pub use childspec::*;
mod childspec;

pub use runner::*;
mod runner;

pub use supervisor::*;
mod supervisor;

pub use spawner::*;
mod spawner;

pub use blueprint::*;
mod blueprint;

pub use child_id::*;
mod child_id;

pub use supervision_address::*;
mod supervision_address;

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Interface, ActorInterface, Debug)]
    #[interface(crate = "crate")]
    pub enum TestInterface {
        Num(Payload<u32>),
    }

    #[derive(Clone, Debug)]
    pub struct TestActor {
        number: u32,
    }

    impl Actor for TestActor {
        type Interface = TestInterface;
        type Exit = ();
        type Error = anyhow::Error;

        async fn exit(&mut self, _: ExitReason) -> Result<Self::Exit, Self::Error> {
            Ok(())
        }
    }

    impl HandleMessage<u32> for TestActor {
        async fn handle_message(
            &mut self,
            _state: &mut ActorState<Self>,
            msg: Payload<u32>,
        ) -> Result<(), Self::Error> {
            println!("Received message: {:?}", msg);
            self.number += msg;
            Ok(())
        }
    }

    pub struct TestActorStarter(u32);

    impl ActorBlueprint for TestActorStarter {
        type Runner = TestActor;

        fn instantiate(&mut self) -> Self::Runner {
            let actor = TestActor { number: self.0 };
            self.0 += 1;
            actor
        }
    }

    #[tokio::test]
    async fn test_map_exit_runnable() {
        // let (runnable, address) = TestRunnable { number: 42 }.extract_address();

        let supervisor = Supervisor::new()
            .with_strategy(SupervisionStrategy::OneForOne)
            .with_intensity(RestartIntensity::default())
            .with_child(ChildSpec::new(
                "ChildA",
                TestActor { number: 42 }.map(|exit| exit.map(|()| 12)).wrap(
                    |inner, stream, address| async move {
                        inner.run(stream, address).await.map(|val| val.to_string())
                    },
                ),
            ))
            .with_child(
                ChildSpec::new("ChildB", TestActor { number: 42 })
                    .mode(RestartMode::Always)
                    .timeout(Duration::from_secs(10)),
            )
            .with_child(ChildSpec::new("ChildD", TestActorStarter(0)))
            .spawn();

        {
            let mut supervisor = Supervisor::new();

            let addr_a = supervisor.add_child(
                ChildSpec::new("ChildA", TestActor { number: 42 }).mode(RestartMode::Always),
            );

            let addr_b = supervisor.add_child(
                ChildSpec::new("ChildB", TestActor { number: 42 }).mode(RestartMode::Always),
            );

            supervisor.spawn();

            let addr_a = addr_a.await.unwrap();
            let addr_b = addr_b.await.unwrap();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        supervisor.signal_shutdown().await.ok();
        supervisor.await.unwrap();
    }
}

#[macro_export]
macro_rules! new_supervisor {
    ($($tt:tt)*) => {
        ()
    };
}
// use new_supervisor;

fn test() {
    let supervisor = crate::new_supervisor!({
        addr = ChildSpec::new("ChildA", TestActor { number: 42 }).mode(RestartMode::Always);

        addr = ChildSpec::new("ChildA", TestActor { number: 42 }).mode(RestartMode::Always);
    });

    let supervisor = crate::new_supervisor! {
        3 +
        3
    };
}
