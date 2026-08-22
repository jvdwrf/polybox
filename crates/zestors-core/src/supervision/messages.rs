use super::*;
use smol_str::{SmolStr, format_smolstr};

#[derive(Message, Debug)]
#[msg(path = "crate", reply = "Vec<ChildDescription>")]
pub struct GetChildren;

#[derive(Message, Debug)]
#[msg(path = "crate", reply = Health)]
pub struct GetHealth;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Health {
    pub status: HealthStatus,
    pub debug_repr: SmolStr,
    pub message: Option<SmolStr>,
    pub details: Vec<HealthDetail>,
}

impl Health {
    pub fn new(status: HealthStatus, debug_repr: impl Debug) -> Self {
        Self {
            status,
            debug_repr: format_smolstr!("{:?}", debug_repr),
            message: None,
            details: Vec::new(),
        }
    }

    pub fn add_message(&mut self, message: impl Into<SmolStr>) -> Option<SmolStr> {
        let old = self.message.take();
        self.message = Some(message.into());
        old
    }

    pub fn with_message(mut self, message: impl Into<SmolStr>) -> Self {
        self.add_message(message);
        self
    }

    pub fn add_detail(&mut self, detail: HealthDetail) {
        self.details.push(detail);
    }

    pub fn with_detail(mut self, detail: HealthDetail) -> Self {
        self.add_detail(detail);
        self
    }
}

impl Display for Health {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.status)?;

        if let Some(message) = &self.message {
            write!(f, ": {}", message)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl HealthStatus {
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    pub fn is_degraded(&self) -> bool {
        matches!(self, HealthStatus::Degraded)
    }

    pub fn is_unhealthy(&self) -> bool {
        matches!(self, HealthStatus::Unhealthy)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HealthStatus::Healthy => "healthy",
            HealthStatus::Degraded => "degraded",
            HealthStatus::Unhealthy => "unhealthy",
        }
    }

    pub fn with_debug_repr(self, value: impl Debug) -> Health {
        Health::new(self, format_smolstr!("{:?}", value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthDetail {
    pub name: SmolStr,
    pub message: SmolStr,
    pub since: Option<jiff::Zoned>,
}

impl HealthDetail {
    pub fn new(name: impl Into<SmolStr>, message: impl Into<SmolStr>) -> Self {
        Self {
            name: name.into(),
            message: message.into(),
            since: None,
        }
    }

    pub fn with_since(mut self, since: jiff::Zoned) -> Self {
        self.since = Some(since);
        self
    }
}

impl Display for HealthDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.message)?;
        if let Some(since) = &self.since {
            write!(f, " (since {})", since)?;
        }
        Ok(())
    }
}
