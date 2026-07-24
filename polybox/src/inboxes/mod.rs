#[cfg(feature = "flume")]
pub use flume_inbox::FlumeInbox;

#[cfg(feature = "tokio")]
pub use tokio_inbox::TokioInbox;

#[cfg(feature = "flume")]
mod flume_inbox;
#[cfg(feature = "tokio")]
mod tokio_inbox;
