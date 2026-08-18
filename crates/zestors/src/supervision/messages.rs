use super::*;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = DebugState)]
pub struct GetDebug;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = "Vec<ChildDescription>")]
pub struct GetChildren;
