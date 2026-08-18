use jiff::Zoned;

#[allow(unused)]
#[derive(utoipa::ToSchema)]
#[schema(
    value_type = String,
    example = "2024-07-04T08:39:00-04:00[America/New_York]"
)]
pub(crate) struct ZonedSchema(Zoned);
