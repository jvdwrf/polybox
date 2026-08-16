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
pub struct SupervisionTree {
    pub abort_timeout: crate::types::DurationSchema,
    #[serde(default, skip_serializing_if = "::ploidy_util::absent::AbsentOr::is_absent")]
    pub children: ::ploidy_util::absent::AbsentOr<
        ::std::vec::Vec<crate::types::SupervisionTree>,
    >,
    #[serde(default, skip_serializing_if = "::ploidy_util::absent::AbsentOr::is_absent")]
    pub debug_state: ::ploidy_util::absent::AbsentOr<crate::types::DebugState>,
    pub pid: crate::types::String,
    pub restart_mode: crate::types::RestartMode,
}
