use crate::_prelude::*;
use smol_str::SmolStr;
use std::{borrow::Cow, fmt::Display, sync::Arc};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Pid {
    Named(SmolStr),
    Random(Uuid),
}

impl Pid {
    pub fn new<T: Into<Self>>(s: T) -> Self {
        s.into()
    }

    pub fn new_static(s: &'static str) -> Self {
        Pid::Named(SmolStr::new_static(s))
    }

    pub fn rand_uuid() -> Self {
        Pid::Random(Uuid::now_v7())
    }
}

impl Default for Pid {
    fn default() -> Self {
        Pid::rand_uuid()
    }
}

impl Display for Pid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pid::Named(s) => write!(f, "{}", s),
            Pid::Random(i) => write!(f, "{}", i),
        }
    }
}

impl From<&'static str> for Pid {
    #[inline]
    fn from(s: &'static str) -> Self {
        Pid::Named(SmolStr::new_static(s))
    }
}

impl From<&mut str> for Pid {
    #[inline]
    fn from(s: &mut str) -> Self {
        Pid::Named(SmolStr::from(s))
    }
}

impl From<&String> for Pid {
    #[inline]
    fn from(s: &String) -> Self {
        Pid::Named(SmolStr::from(s))
    }
}

impl From<String> for Pid {
    #[inline(always)]
    fn from(text: String) -> Self {
        Pid::Named(SmolStr::from(text))
    }
}

impl From<Box<str>> for Pid {
    #[inline]
    fn from(s: Box<str>) -> Pid {
        Pid::Named(SmolStr::from(s))
    }
}

impl From<Arc<str>> for Pid {
    #[inline]
    fn from(s: Arc<str>) -> Pid {
        Pid::Named(SmolStr::from(s))
    }
}

impl<'a> From<Cow<'a, str>> for Pid {
    #[inline]
    fn from(s: Cow<'a, str>) -> Pid {
        Pid::Named(SmolStr::from(s))
    }
}
