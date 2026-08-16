pub mod channel;
pub use channel::spawn;

pub mod messaging;

pub mod signals;

pub(crate) mod schemas;

pub(crate) mod _prelude {
    pub(crate) use crate::{channel::*, messaging::*, signals::*};
    pub(crate) use polybox_codegen::{Interface, Message};
    pub(crate) use rootcause::Report;
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::{future::Future, time::Duration};
    pub(crate) use type_sets::Set;
}

pub mod prelude {
    pub use crate::{
        channel::{ActorRef as _, Address, Channel, Child, EventStream, IntoDyn as _, Pid, spawn},
        messaging::{Interface, Message, Payload, Sends as _, type_sets::Set},
        signals::{Event, SignalEvent},
    };
}
