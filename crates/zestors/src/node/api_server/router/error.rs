use super::*;

pub(super) type ApiResult<T = Response> = Result<T, ApiError>;

pub(super) struct ApiError {
    pub status: StatusCode,
    pub error: Report,
}

pub(super) fn not_found(msg: &str) -> ApiError {
    ApiError::not_found(report!("{msg}"))
}

impl ApiError {
    pub fn new(status: StatusCode, error: Report) -> Self {
        Self { status, error }
    }

    pub fn not_found(error: Report) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error,
        }
    }

    pub fn internal_server_error(error: Report) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({
            "error": self.error.to_string(),
        }));
        (self.status, body).into_response()
    }
}

impl<T> From<T> for ApiError
where
    T: Into<Report>,
{
    fn from(error: T) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: error.into(),
        }
    }
}
