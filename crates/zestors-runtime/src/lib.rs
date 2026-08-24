pub mod channel;
pub use channel::spawn;

pub mod messaging;

pub mod signals;

#[allow(unused_imports)]
pub(crate) mod _prelude {
    pub(crate) use crate::{channel::*, messaging::*, signals::*};
    pub(crate) use rootcause::Report;
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::{future::Future, time::Duration};
    pub(crate) use type_sets::Set;
    pub(crate) use zestors_codegen::{Interface, Message};
}

pub mod prelude {
    pub use crate::{
        channel::{ActorRef as _, Address, Channel, Child, Inbox, IntoDyn as _, Pid, spawn},
        messaging::{Envelope, Interface, Message, Sends as _, type_sets::Set},
        signals::{Event, Signal},
    };
}
