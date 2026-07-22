//! The core library for the `polybox` crate.
//!
//! See [GitHub](https://github.com/jvdwrf/polybox) for more information.

pub mod errors;
mod inbox;
mod interface;
mod message;
pub mod oneshot;
mod payload;
mod sends;

pub use polybox_codegen::{Interface, Message};
pub use type_sets;
pub use {inbox::*, interface::*, message::*, payload::*, sends::*};

pub(crate) use {errors::*, oneshot::*, type_sets::*};
