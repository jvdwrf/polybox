//! The core library for the `polybox` crate.
//!
//! See [GitHub](https://github.com/jvdwrf/polybox) for more information.

pub mod errors;
mod interface;
mod message;
pub mod oneshot;
mod payload;
mod sender;
mod sends;

pub use type_sets;
pub use {interface::*, message::*, payload::*, sender::*, sends::*};

pub(crate) use {errors::*, oneshot::*, type_sets::*};
