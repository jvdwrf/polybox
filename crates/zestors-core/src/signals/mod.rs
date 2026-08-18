use crate::{_prelude::*, schemas::DurationSchema};
use polybox_codegen::Message;

mod interface;
pub(crate) use interface::*;

mod event;
pub use event::*;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = DebugState)]
pub struct GetDebug;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = "Vec<ChildDescription>")]
pub struct GetChildren;
