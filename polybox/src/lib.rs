//! Message-passing abstractions to make working with channels and actors a more seamless experience.
//!
//!
//!
//! # Defining messages
//! ```rust
//! # use polybox::*;
//! #
//! #[derive(Message)]
//! struct MyMessage;
//!
//! #[derive(Message)]
//! #[msg(reply = String)]
//! struct MyRequest;
//! ```
//!
//!
//!
//! # Defining an interface
//! ```rust
//! # use polybox::*;
//! #
//! # #[derive(Message)]
//! # struct MyMessage;
//! #
//! # #[derive(Message)]
//! # #[msg(reply = String)]
//! # struct MyRequest;
//! #
//! #[derive(Interface)]
//! enum MyInterface {
//!     Number(Payload<u32>),
//!     MyMessage(Payload<MyMessage>),
//!     MyRequest(Payload<MyRequest>),
//! }
//! ```
//!
//!
//!
//! # Sending messages
//! ```rust
//! # use polybox::{*, inboxes::TokioInbox};
//! #
//! # #[derive(Message, Debug)]
//! # struct MyMessage;
//! #
//! # #[derive(Message, Debug)]
//! # #[msg(reply = String)]
//! # struct MyRequest;
//! #
//! # #[derive(Interface, Debug)]
//! # enum MyInterface {
//! #     Number(Payload<u32>),
//! #     MyMessage(Payload<MyMessage>),
//! #     MyRequest(Payload<MyRequest>),
//! # }
//! #
//! # #[tokio::main]
//! # async fn main() {
//! let (inbox, mut receiver) = TokioInbox::<MyInterface>::new(1000);
//!
//! inbox.send(42_u32).await.unwrap();
//! inbox.send(MyMessage).await.unwrap();
//! let _reply = inbox.request(MyRequest).await.unwrap();
//! # }
//! ```
//!
//!
//!
//! # Dynamic inboxes
//! ```rust
//! # use polybox::{*, inboxes::TokioInbox};
//! #
//! # #[derive(Message, Debug)]
//! # struct MyMessage;
//! #
//! # #[derive(Message, Debug)]
//! # #[msg(reply = String)]
//! # struct MyRequest;
//! #
//! # #[derive(Interface, Debug)]
//! # enum MyInterface {
//! #     Number(Payload<u32>),
//! #     MyMessage(Payload<MyMessage>),
//! #     MyRequest(Payload<MyRequest>),
//! # }
//! #
//! # #[tokio::main]
//! # async fn main() {
//! let (inbox, mut receiver) = TokioInbox::<MyInterface>::new(1000);
//!
//! inbox.send(42_u32).await.unwrap();
//! inbox.send(MyMessage).await.unwrap();
//! let _reply = inbox.request(MyRequest).await.unwrap();
//! # }
//! ```

pub mod inboxes;

pub use polybox_codegen::{Interface, Message};
pub use polybox_core::*;
