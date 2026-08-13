use crate::_prelude::*;
use axum::extract::{Query, State};
use axum_autoroute::{AutorouteApiRouter, autoroute, method_routers};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use utoipa::IntoParams;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone, Debug)]
pub struct ApiConfig {
    pub addr: SocketAddr,
    pub swagger_ui: bool,
    pub pid: Pid,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8080".parse().unwrap(),
            swagger_ui: true,
            pid: Pid::new("api_server"),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ApiState {
    pub root_supervisor: Pid,
    pub cfg: Arc<ApiConfig>,
}

impl Actor for ApiState {
    type Interface = ();
    type Exit = ();

    async fn run(
        self,
        mut stream: EventStream<Self::Interface>,
        _: Address<Self::Interface>,
    ) -> Result<Self::Exit, anyhow::Error> {
        let run_fut = self.clone().run();

        tokio::select! {
            res = run_fut => res,
            _ = stream.recv() => {
                tracing::info!("API server received exit signal");
                Ok(())
            }
        }?;

        Ok(())
    }
}

impl ApiState {
    pub fn new(cfg: ApiConfig, root_supervisor: Pid) -> Self {
        Self {
            root_supervisor,
            cfg: Arc::new(cfg),
        }
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        let (router, api) = AutorouteApiRouter::new()
            .with_pub_routes(method_routers!(get_tree))
            .with_state(self.clone())
            .split_for_parts();

        let router = match self.cfg.swagger_ui {
            true => router
                .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api.clone())),
            false => router,
        };

        let listener = TcpListener::bind(self.cfg.addr).await?;

        tracing::info!("API server running at http://{}", self.cfg.addr);

        axum::serve(listener, router.into_make_service()).await?;

        Ok(())
    }
}

#[derive(IntoParams, Deserialize)]
struct StartTreeFrom {
    /// The PID from which to start the supervision tree. If not provided, the root supervisor will be used.
    pid: Option<Pid>,
}

#[derive(IntoParams, Deserialize)]
struct WithDebugParam {
    /// Whether to include debug state in the supervision tree
    include_debug: Option<bool>,
}

#[autoroute(GET, path = "/tree", responses = [
    (OK, body = SupervisionTree),
    (NOT_FOUND, body = &'static str, serializer=NONE),
    (INTERNAL_SERVER_ERROR, body = &'static str, serializer=NONE),
])]
async fn get_tree(
    Query(include_debug): Query<WithDebugParam>,
    Query(pid): Query<StartTreeFrom>,
    State(state): State<ApiState>,
) -> _ {
    tracing::debug!(
        "Received request for supervision tree with PID: {:?} and query: {:?}",
        pid.pid,
        include_debug.include_debug
    );

    let include_debug = include_debug.include_debug.unwrap_or(false);

    let pid = pid.pid.unwrap_or_else(|| state.root_supervisor.clone());

    let tree = match SupervisionTree::new_populated(pid).await {
        Ok(tree) => tree,
        Err(_) => return "Failed to populate supervision tree".into_internal_server_error(),
    };

    let tree = if include_debug {
        match tree.with_debug_state().await {
            Ok(tree) => tree,
            Err(_) => return "Failed to populate debug state".into_internal_server_error(),
        }
    } else {
        tree
    };

    tree.into_ok()
}
