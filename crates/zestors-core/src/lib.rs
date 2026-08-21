use crate::_prelude::*;
use std::sync::Arc;

pub use zestors_channel::*;

pub mod handler;
pub mod node;
pub mod registry;
pub mod supervision;
pub use zestors_codegen::{HandlerInterface, Interface, Message};

pub(crate) mod _prelude {
    #![allow(unused_imports)]
    pub(crate) use crate::{handler::*, node::*, registry::*, supervision::*, *};
    pub(crate) use zestors_channel::{
        channel::{errors::*, *},
        messaging::*,
        signals::*,
    };

    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::{
        fmt::{Debug, Display},
        time::Duration,
    };

    pub(crate) use rootcause::Report;
    pub(crate) use type_sets::Set;
    pub(crate) use zestors_channel::*;
}

pub(crate) mod schemas;

pub mod prelude {
    pub use crate::supervision::{Actor, Blueprint};
    pub use zestors_channel::prelude::*;
    pub use zestors_codegen::{Interface, Message};
}
