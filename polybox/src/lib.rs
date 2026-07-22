//! Message-passing abstractions to make working with channels and actors a more seamless experience.
//!
//! See [GitHub](https://github.com/jvdwrf/polybox) for more information.

#[cfg(feature = "flume")]
mod flume_inbox;
#[cfg(feature = "flume")]
pub use flume_inbox::FlumeInbox;

#[cfg(feature = "tokio")]
pub use tokio_inbox::TokioInbox;
#[cfg(feature = "tokio")]
mod tokio_inbox;

pub use polybox_codegen::{Interface, Message};
pub use polybox_core::*;
