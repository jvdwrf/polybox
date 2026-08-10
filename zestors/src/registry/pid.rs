use crate::_prelude::*;
use smol_str::SmolStr;
use std::{borrow::Cow, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Pid {
    Named(SmolStr),
    Indexed(u64),
}

impl Pid {
    pub fn new<T: Into<Self>>(s: T) -> Self {
        s.into()
    }

    pub fn new_static(s: &'static str) -> Self {
        Pid::Named(SmolStr::new_static(s))
    }
}

impl Display for Pid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pid::Named(s) => write!(f, "{}", s),
            Pid::Indexed(i) => write!(f, "{}", i),
        }
    }
}

impl From<&str> for Pid {
    #[inline]
    fn from(s: &str) -> Self {
        Pid::Named(SmolStr::from(s))
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

impl From<usize> for Pid {
    fn from(value: usize) -> Self {
        Pid::Indexed(value as u64)
    }
}

impl From<u64> for Pid {
    fn from(value: u64) -> Self {
        Pid::Indexed(value)
    }
}

impl From<u32> for Pid {
    fn from(value: u32) -> Self {
        Pid::Indexed(value.into())
    }
}

impl From<u16> for Pid {
    fn from(value: u16) -> Self {
        Pid::Indexed(value.into())
    }
}

impl From<u8> for Pid {
    fn from(value: u8) -> Self {
        Pid::Indexed(value.into())
    }
}
