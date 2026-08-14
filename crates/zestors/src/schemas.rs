use crate::_prelude::*;

#[derive(utoipa::ToSchema, Serialize, Deserialize)]
pub(crate) struct DurationSchema {
    secs: u64,
    nanos: u32,
}

impl DurationSchema {
    pub fn deserializer<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = DurationSchema::deserialize(deserializer)?;
        Ok(Duration::new(value.secs, value.nanos))
    }

    pub fn serializer<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = DurationSchema {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        };
        value.serialize(serializer)
    }
}
