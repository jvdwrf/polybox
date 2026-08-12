use crate::_prelude::*;
use bs58::Alphabet;
use smol_str::SmolStr;
use std::{borrow::Cow, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Pid(SmolStr);

impl Pid {
    pub fn new<T: Into<Self>>(s: T) -> Self {
        s.into()
    }

    pub fn new_static(s: &'static str) -> Self {
        Pid(SmolStr::new_static(s))
    }

    pub fn rand() -> Self {
        let rand: [u8; 12] = rand::random();

        let mut val = String::with_capacity(17);
        bs58::encode(rand)
            .with_alphabet(&Alphabet::BITCOIN)
            .onto(&mut val)
            .expect("Capacity is sufficient");
        val.truncate(16);

        Self::new(val)
    }
}

impl Default for Pid {
    fn default() -> Self {
        Pid::rand()
    }
}

impl Display for Pid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&'static str> for Pid {
    #[inline]
    fn from(s: &'static str) -> Self {
        Pid(SmolStr::new_static(s))
    }
}

impl From<&mut str> for Pid {
    #[inline]
    fn from(s: &mut str) -> Self {
        Pid(SmolStr::from(s))
    }
}

impl From<&String> for Pid {
    #[inline]
    fn from(s: &String) -> Self {
        Pid(SmolStr::from(s))
    }
}

impl From<String> for Pid {
    #[inline(always)]
    fn from(text: String) -> Self {
        Pid(SmolStr::from(text))
    }
}

impl From<Box<str>> for Pid {
    #[inline]
    fn from(s: Box<str>) -> Pid {
        Pid(SmolStr::from(s))
    }
}

impl From<Arc<str>> for Pid {
    #[inline]
    fn from(s: Arc<str>) -> Pid {
        Pid(SmolStr::from(s))
    }
}

impl<'a> From<Cow<'a, str>> for Pid {
    #[inline]
    fn from(s: Cow<'a, str>) -> Pid {
        Pid(SmolStr::from(s))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pid() {
        for i in 0..100 {
            let pid = Pid::rand();
            println!("pid {}: {}", i, pid);
        }

        let pid1 = Pid::rand();
        let pid2 = Pid::rand();
        assert_ne!(pid1, pid2);
    }
}
