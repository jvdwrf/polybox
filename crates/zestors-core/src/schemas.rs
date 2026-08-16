use crate::_prelude::*;

#[derive(utoipa::ToSchema, Serialize, Deserialize)]
#[schema(example = "{\"secs\": 5, \"nanos\": 0}")]
pub(crate) struct DurationSchema {
    secs: u64,
    nanos: u32,
}

impl DurationSchema {
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <DurationSchema as Deserialize>::deserialize(deserializer)?;
        Ok(Duration::new(value.secs, value.nanos))
    }

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
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
