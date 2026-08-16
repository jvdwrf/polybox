use crate::_prelude::*;
use std::sync::Arc;

pub use zestors_core::*;

pub mod handler;
pub mod node;
pub mod registry;
pub mod supervision;
pub use ::type_sets;
pub use polybox_codegen::{
    HandlerInterface, InterfaceZestors as Interface, MessageZestors as Message,
};

pub(crate) mod _prelude {
    #![allow(unused_imports)]
    pub(crate) use crate::{handler::*, node::*, registry::*, supervision::*, *};
    pub(crate) use zestors_core::{channel::*, messaging::*, signals::*};

    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::{
        fmt::{Debug, Display},
        time::Duration,
    };

    pub(crate) use rootcause::Report;
    pub(crate) use type_sets::Set;
    pub(crate) use zestors_core::*;
}

pub(crate) mod schemas;
