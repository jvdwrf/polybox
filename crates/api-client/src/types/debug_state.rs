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
pub struct DebugState {
    pub description: ::std::string::String,
    pub status: crate::types::ActorStatus,
    pub uptime: crate::types::DurationSchema,
}
