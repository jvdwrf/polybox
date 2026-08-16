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
pub enum ExitError {
    Panic,
    Abort,
    UnhandledError,
    OtherExitError(String),
}
impl ::std::default::Default for ExitError {
    fn default() -> Self {
        Self::OtherExitError(::std::string::String::default())
    }
}
impl ::std::fmt::Display for ExitError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.write_str(
            match self {
                Self::Panic => "Panic",
                Self::Abort => "Abort",
                Self::UnhandledError => "UnhandledError",
                Self::OtherExitError(s) => s.as_str(),
            },
        )
    }
}
impl ::std::str::FromStr for ExitError {
    type Err = ::std::convert::Infallible;
    fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
        ::std::result::Result::Ok(
            match s {
                "Panic" => Self::Panic,
                "Abort" => Self::Abort,
                "UnhandledError" => Self::UnhandledError,
                _ => Self::OtherExitError(s.to_owned()),
            },
        )
    }
}
impl<'de> ::ploidy_util::serde::Deserialize<'de> for ExitError {
    fn deserialize<D: ::ploidy_util::serde::Deserializer<'de>>(
        deserializer: D,
    ) -> ::std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> ::ploidy_util::serde::de::Visitor<'de> for Visitor {
            type Value = ExitError;
            fn expecting(
                &self,
                f: &mut ::std::fmt::Formatter<'_>,
            ) -> ::std::fmt::Result {
                f.write_str("a variant of `ExitError`")
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
impl ::ploidy_util::serde::Serialize for ExitError {
    fn serialize<S: ::ploidy_util::serde::Serializer>(
        &self,
        serializer: S,
    ) -> ::std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}
