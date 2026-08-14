//! The core library for the `polybox` crate.
//!
//! See [GitHub](https://github.com/jvdwrf/polybox) for more information.

mod dyn_sender;
pub mod errors;
mod interface;
mod message;
pub mod oneshot;
mod payload;
mod poly_sender;
mod sends;

pub use type_sets;
pub use {dyn_sender::*, interface::*, message::*, payload::*, poly_sender::*, sends::*};

pub(crate) use {errors::*, oneshot::*, type_sets::*};
