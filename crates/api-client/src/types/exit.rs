#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    ::ploidy_util::serde::Serialize,
    ::ploidy_util::serde::Deserialize,
    ::ploidy_util::pointer::JsonPointee,
    ::ploidy_util::pointer::JsonPointerTarget
)]
#[serde(crate = "::ploidy_util::serde", untagged)]
#[ploidy(pointer(crate = "::ploidy_util::pointer", untagged))]
pub enum Exit {
    Exit1(crate::types::exit::types::Exit1),
    Exit2(crate::types::exit::types::Exit2),
}
pub mod types {
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
    pub enum Exit1 {
        Normal,
        OtherExit1(String),
    }
    impl ::std::default::Default for Exit1 {
        fn default() -> Self {
            Self::OtherExit1(::std::string::String::default())
        }
    }
    impl ::std::fmt::Display for Exit1 {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            f.write_str(
                match self {
                    Self::Normal => "Normal",
                    Self::OtherExit1(s) => s.as_str(),
                },
            )
        }
    }
    impl ::std::str::FromStr for Exit1 {
        type Err = ::std::convert::Infallible;
        fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
            ::std::result::Result::Ok(
                match s {
                    "Normal" => Self::Normal,
                    _ => Self::OtherExit1(s.to_owned()),
                },
            )
        }
    }
    impl<'de> ::ploidy_util::serde::Deserialize<'de> for Exit1 {
        fn deserialize<D: ::ploidy_util::serde::Deserializer<'de>>(
            deserializer: D,
        ) -> ::std::result::Result<Self, D::Error> {
            struct Visitor;
            impl<'de> ::ploidy_util::serde::de::Visitor<'de> for Visitor {
                type Value = Exit1;
                fn expecting(
                    &self,
                    f: &mut ::std::fmt::Formatter<'_>,
                ) -> ::std::fmt::Result {
                    f.write_str("a variant of `Exit1`")
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
    impl ::ploidy_util::serde::Serialize for Exit1 {
        fn serialize<S: ::ploidy_util::serde::Serializer>(
            &self,
            serializer: S,
        ) -> ::std::result::Result<S::Ok, S::Error> {
            serializer.collect_str(self)
        }
    }
    #[derive(
        Debug,
        Clone,
        PartialEq,
        Eq,
        Hash,
        Default,
        ::ploidy_util::serde::Serialize,
        ::ploidy_util::serde::Deserialize,
        ::ploidy_util::pointer::JsonPointee,
        ::ploidy_util::pointer::JsonPointerTarget
    )]
    #[serde(crate = "::ploidy_util::serde")]
    #[ploidy(pointer(crate = "::ploidy_util::pointer"))]
    pub struct Exit2 {
        #[serde(rename = "Error")]
        #[ploidy(pointer(rename = "Error"))]
        pub error: crate::types::ExitError,
    }
}
