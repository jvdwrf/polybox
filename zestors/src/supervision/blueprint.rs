use super::*;

pub trait ActorBlueprint {
    type Runner: ActorRunner;

    fn create_runner(&mut self) -> Self::Runner;
}

impl<T: ActorRunner + Clone> ActorBlueprint for T {
    type Runner = T;

    fn create_runner(&mut self) -> Self::Runner {
        self.clone()
    }
}
