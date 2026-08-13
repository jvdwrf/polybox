use crate::_prelude::*;
use axum::extract::{Path, State};
use axum_autoroute::{AutorouteApiRouter, autoroute, method_routers};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use utoipa::IntoParams;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub(super) struct ApiClient {
    pub socket_addr: SocketAddr,
    pub root_supervisor: Address<SupervisorInterface>,
}

impl ApiClient {
    pub fn new(socket_addr: SocketAddr, root_supervisor: Address<SupervisorInterface>) -> Self {
        Self {
            socket_addr,
            root_supervisor,
        }
    }

    pub async fn run_api(self) -> Result<(), anyhow::Error> {
        let cfg = Arc::new(self);

        let (router, api) = AutorouteApiRouter::new()
            .with_pub_routes(method_routers!(get_tree))
            .with_state(cfg.clone())
            .split_for_parts();

        let router =
            router.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api.clone()));

        println!("API server running at http://{}", cfg.socket_addr);

        let listener = TcpListener::bind(cfg.socket_addr).await?;

        axum::serve(listener, router.into_make_service()).await?;

        Ok(())
    }
}

#[derive(IntoParams, Deserialize)]
struct OptPidParam {
    /// The PID
    pid: Option<Pid>,
}

#[autoroute(GET, path = "/tree/{pid}", responses = [
    (OK, body = SupervisionTree),
    (NOT_FOUND, body = &'static str, serializer=NONE),
    (INTERNAL_SERVER_ERROR, body = &'static str, serializer=NONE),
])]
async fn get_tree(path: Path<OptPidParam>, State(state): State<Arc<ApiClient>>) -> _ {
    tracing::debug!(
        "Received request for supervision tree with PID: {:?}",
        path.0.pid
    );

    let address = match path.0.pid {
        Some(pid) => match Registry::local().get(&pid) {
            Some(addr) => addr,
            None => return "PID not found".into_not_found(),
        },
        None => state.root_supervisor.clone().into_dyn(),
    };

    let tree = match SupervisionTree::new_populated(address.pid().clone()).await {
        Ok(tree) => tree,
        Err(_) => return "Failed to populate supervision tree".into_internal_server_error(),
    };

    let tree = match tree.with_debug_state().await {
        Ok(tree) => tree,
        Err(_) => return "Failed to populate debug state".into_internal_server_error(),
    };

    tree.into_ok()
}
