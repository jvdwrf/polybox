#[derive(
    Clone,
    Debug,
    Eq,
    Hash,
    PartialEq,
    ::ploidy_util::pointer::JsonPointee,
    ::ploidy_util::pointer::JsonPointerTarget
)]
#[ploidy(pointer(crate = "::ploidy_util::pointer"))]
pub enum RestartMode {
    Always,
    OnError,
    Never,
    OtherRestartMode(String),
}
impl ::std::default::Default for RestartMode {
    fn default() -> Self {
        Self::OtherRestartMode(::std::string::String::default())
    }
}
impl ::std::fmt::Display for RestartMode {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.write_str(
            match self {
                Self::Always => "Always",
                Self::OnError => "OnError",
                Self::Never => "Never",
                Self::OtherRestartMode(s) => s.as_str(),
            },
        )
    }
}
impl ::std::str::FromStr for RestartMode {
    type Err = ::std::convert::Infallible;
    fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
        ::std::result::Result::Ok(
            match s {
                "Always" => Self::Always,
                "OnError" => Self::OnError,
                "Never" => Self::Never,
                _ => Self::OtherRestartMode(s.to_owned()),
            },
        )
    }
}
impl<'de> ::ploidy_util::serde::Deserialize<'de> for RestartMode {
    fn deserialize<D: ::ploidy_util::serde::Deserializer<'de>>(
        deserializer: D,
    ) -> ::std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> ::ploidy_util::serde::de::Visitor<'de> for Visitor {
            type Value = RestartMode;
            fn expecting(
                &self,
                f: &mut ::std::fmt::Formatter<'_>,
            ) -> ::std::fmt::Result {
                f.write_str("a variant of `RestartMode`")
            }
            fn visit_str<E: ::ploidy_util::serde::de::Error>(
                self,
                s: &str,
            ) -> ::std::result::Result<Self::Value, E> {
                let ::std::result::Result::Ok(v) = ::std::str::FromStr::from_str(s);
                Ok(v)
            }
        }
        ::ploidy_util::serde::Deserializer::deserialize_str(deserializer, Visitor)
    }
}
impl ::ploidy_util::serde::Serialize for RestartMode {
    fn serialize<S: ::ploidy_util::serde::Serializer>(
        &self,
        serializer: S,
    ) -> ::std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}
