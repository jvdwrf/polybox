pub mod channel;
pub mod messaging;
pub mod registry;
pub mod signals;

pub use channel::{spawn, spawn_with};

#[allow(unused_imports)]
pub(crate) mod _prelude {
    pub(crate) use crate::{channel::*, messaging::*, registry::*, signals::*};
    pub(crate) use rootcause::Report;
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::{future::Future, time::Duration};
    pub(crate) use type_sets::{Set, TypeSet};
    pub(crate) use zestors_codegen::{Interface, Message};
}

pub mod prelude {
    pub use crate::{
        channel::{
            ActorOpsExt as _, Address, StrongAddress, Child, Inbox, IntoDyn as _, Pid, spawn_with,
        },
        messaging::{Envelope, Interface, Message, Sends as _, type_sets::Set},
        signals::{Event, Signal},
    };
}
