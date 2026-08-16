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
pub enum ActorStatus {
    ActorStatus1(crate::types::actor_status::types::ActorStatus1),
    ActorStatus2(crate::types::actor_status::types::ActorStatus2),
    ActorStatus3(crate::types::actor_status::types::ActorStatus3),
    ActorStatus4(crate::types::actor_status::types::ActorStatus4),
    ActorStatus5(crate::types::actor_status::types::ActorStatus5),
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
    pub enum ActorStatus1 {
        Initializing,
        OtherActorStatus1(String),
    }
    impl ::std::default::Default for ActorStatus1 {
        fn default() -> Self {
            Self::OtherActorStatus1(::std::string::String::default())
        }
    }
    impl ::std::fmt::Display for ActorStatus1 {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            f.write_str(
                match self {
                    Self::Initializing => "initializing",
                    Self::OtherActorStatus1(s) => s.as_str(),
                },
            )
        }
    }
    impl ::std::str::FromStr for ActorStatus1 {
        type Err = ::std::convert::Infallible;
        fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
            ::std::result::Result::Ok(
                match s {
                    "initializing" => Self::Initializing,
                    _ => Self::OtherActorStatus1(s.to_owned()),
                },
            )
        }
    }
    impl<'de> ::ploidy_util::serde::Deserialize<'de> for ActorStatus1 {
        fn deserialize<D: ::ploidy_util::serde::Deserializer<'de>>(
            deserializer: D,
        ) -> ::std::result::Result<Self, D::Error> {
            struct Visitor;
            impl<'de> ::ploidy_util::serde::de::Visitor<'de> for Visitor {
                type Value = ActorStatus1;
                fn expecting(
                    &self,
                    f: &mut ::std::fmt::Formatter<'_>,
                ) -> ::std::fmt::Result {
                    f.write_str("a variant of `ActorStatus1`")
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
    impl ::ploidy_util::serde::Serialize for ActorStatus1 {
        fn serialize<S: ::ploidy_util::serde::Serializer>(
            &self,
            serializer: S,
        ) -> ::std::result::Result<S::Ok, S::Error> {
            serializer.collect_str(self)
        }
    }
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
    pub enum ActorStatus2 {
        Running,
        OtherActorStatus2(String),
    }
    impl ::std::default::Default for ActorStatus2 {
        fn default() -> Self {
            Self::OtherActorStatus2(::std::string::String::default())
        }
    }
    impl ::std::fmt::Display for ActorStatus2 {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            f.write_str(
                match self {
                    Self::Running => "running",
                    Self::OtherActorStatus2(s) => s.as_str(),
                },
            )
        }
    }
    impl ::std::str::FromStr for ActorStatus2 {
        type Err = ::std::convert::Infallible;
        fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
            ::std::result::Result::Ok(
                match s {
                    "running" => Self::Running,
                    _ => Self::OtherActorStatus2(s.to_owned()),
                },
            )
        }
    }
    impl<'de> ::ploidy_util::serde::Deserialize<'de> for ActorStatus2 {
        fn deserialize<D: ::ploidy_util::serde::Deserializer<'de>>(
            deserializer: D,
        ) -> ::std::result::Result<Self, D::Error> {
            struct Visitor;
            impl<'de> ::ploidy_util::serde::de::Visitor<'de> for Visitor {
                type Value = ActorStatus2;
                fn expecting(
                    &self,
                    f: &mut ::std::fmt::Formatter<'_>,
                ) -> ::std::fmt::Result {
                    f.write_str("a variant of `ActorStatus2`")
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
    impl ::ploidy_util::serde::Serialize for ActorStatus2 {
        fn serialize<S: ::ploidy_util::serde::Serializer>(
            &self,
            serializer: S,
        ) -> ::std::result::Result<S::Ok, S::Error> {
            serializer.collect_str(self)
        }
    }
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
    pub enum ActorStatus3 {
        Suspended,
        OtherActorStatus3(String),
    }
    impl ::std::default::Default for ActorStatus3 {
        fn default() -> Self {
            Self::OtherActorStatus3(::std::string::String::default())
        }
    }
    impl ::std::fmt::Display for ActorStatus3 {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            f.write_str(
                match self {
                    Self::Suspended => "suspended",
                    Self::OtherActorStatus3(s) => s.as_str(),
                },
            )
        }
    }
    impl ::std::str::FromStr for ActorStatus3 {
        type Err = ::std::convert::Infallible;
        fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
            ::std::result::Result::Ok(
                match s {
                    "suspended" => Self::Suspended,
                    _ => Self::OtherActorStatus3(s.to_owned()),
                },
            )
        }
    }
    impl<'de> ::ploidy_util::serde::Deserialize<'de> for ActorStatus3 {
        fn deserialize<D: ::ploidy_util::serde::Deserializer<'de>>(
            deserializer: D,
        ) -> ::std::result::Result<Self, D::Error> {
            struct Visitor;
            impl<'de> ::ploidy_util::serde::de::Visitor<'de> for Visitor {
                type Value = ActorStatus3;
                fn expecting(
                    &self,
                    f: &mut ::std::fmt::Formatter<'_>,
                ) -> ::std::fmt::Result {
                    f.write_str("a variant of `ActorStatus3`")
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
    impl ::ploidy_util::serde::Serialize for ActorStatus3 {
        fn serialize<S: ::ploidy_util::serde::Serializer>(
            &self,
            serializer: S,
        ) -> ::std::result::Result<S::Ok, S::Error> {
            serializer.collect_str(self)
        }
    }
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
    pub enum ActorStatus4 {
        Exiting,
        OtherActorStatus4(String),
    }
    impl ::std::default::Default for ActorStatus4 {
        fn default() -> Self {
            Self::OtherActorStatus4(::std::string::String::default())
        }
    }
    impl ::std::fmt::Display for ActorStatus4 {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            f.write_str(
                match self {
                    Self::Exiting => "exiting",
                    Self::OtherActorStatus4(s) => s.as_str(),
                },
            )
        }
    }
    impl ::std::str::FromStr for ActorStatus4 {
        type Err = ::std::convert::Infallible;
        fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
            ::std::result::Result::Ok(
                match s {
                    "exiting" => Self::Exiting,
                    _ => Self::OtherActorStatus4(s.to_owned()),
                },
            )
        }
    }
    impl<'de> ::ploidy_util::serde::Deserialize<'de> for ActorStatus4 {
        fn deserialize<D: ::ploidy_util::serde::Deserializer<'de>>(
            deserializer: D,
        ) -> ::std::result::Result<Self, D::Error> {
            struct Visitor;
            impl<'de> ::ploidy_util::serde::de::Visitor<'de> for Visitor {
                type Value = ActorStatus4;
                fn expecting(
                    &self,
                    f: &mut ::std::fmt::Formatter<'_>,
                ) -> ::std::fmt::Result {
                    f.write_str("a variant of `ActorStatus4`")
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
    impl ::ploidy_util::serde::Serialize for ActorStatus4 {
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
        ::ploidy_util::serde::Serialize,
        ::ploidy_util::serde::Deserialize,
        ::ploidy_util::pointer::JsonPointee,
        ::ploidy_util::pointer::JsonPointerTarget
    )]
    #[serde(crate = "::ploidy_util::serde")]
    #[ploidy(pointer(crate = "::ploidy_util::pointer"))]
    pub struct ActorStatus5 {
        pub dead: crate::types::Exit,
    }
}
