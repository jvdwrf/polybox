use super::*;

#[derive(Message, Debug)]
#[msg(path = "crate", reply = DebugState)]
pub struct GetDebug;

#[derive(Message, Debug)]
#[msg(path = "crate", reply = "Vec<ChildDescription>")]
pub struct GetChildren;

#[derive(Message, Debug)]
#[msg(path = "crate", reply = "HealthStatus")]
pub struct GetHealth;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}
