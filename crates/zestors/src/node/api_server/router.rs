use crate::_prelude::*;
use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use axum_typed_routing::{TypedRouter, route};

impl ApiServer {
    pub(super) fn create_router(&self) -> Router {
        Router::new().typed_route(get_tree).with_state(self.clone())
    }
}

#[route(GET "/tree?pid&include_debug" with ApiServer)]
async fn get_tree(pid: Option<Pid>, include_debug: Option<bool>) -> Response {
    tracing::debug!(
        "Received request for supervision tree with PID: {:?} and query: {:?}",
        pid,
        include_debug
    );

    let include_debug = include_debug.unwrap_or(false);

    let Some(pid) = pid.or_else(|| Node::root_supervisor_pid().cloned()) else {
        return (
            StatusCode::BAD_REQUEST,
            "No PID provided and no root supervisor PID found",
        )
            .into_response();
    };

    let address = Registry::local().get(&pid);
    if address.is_none() {
        return (StatusCode::NOT_FOUND, "PID not found in registry").into_response();
    }

    let tree = SupervisionTree::new(ChildDescription {
        pid,
        cfg: ChildConfig {
            restart_mode: RestartMode::Always,
            abort_timeout: Duration::from_secs(10),
            init_timeout: Duration::from_secs(10),
        },
    })
    .populated(Duration::from_millis(50))
    .await
    .populated_channel_snapshots();

    let tree = if include_debug {
        tree.populated_debug_state(Duration::from_millis(50)).await
    } else {
        tree
    };

    (StatusCode::OK, Json(tree)).into_response()
}
