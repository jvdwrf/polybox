use crate::_prelude::*;
use axum::extract::{Query, State};
use axum_autoroute::{AutorouteApiRouter, autoroute, method_routers};
use std::{convert::Infallible, net::SocketAddr, pin::pin};
use tokio::net::TcpListener;
use utoipa::IntoParams;
// use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone, Debug)]
pub struct ApiConfig {
    pub addr: SocketAddr,
    pub swagger_ui: bool,
    pub pid: Pid,
}

impl ApiConfig {
    pub fn generate_openapi() -> utoipa::openapi::OpenApi {
        ApiServer::new(Self::default(), Pid::new(""))
            .create_router()
            .split_for_parts()
            .1
    }
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
pub(super) struct ApiServer {
    pub root_supervisor: Pid,
    pub cfg: Arc<ApiConfig>,
}

impl Actor for ApiServer {
    type Interface = Infallible;
    type Exit = ();

    async fn run(self, mut state: EventStream<Self::Interface>) -> Result<Self::Exit, Report> {
        let mut run_api = pin!(self.clone().run());

        loop {
            let event = tokio::select! {
                api_exit = &mut run_api => {
                    match &api_exit {
                        Ok(_) => {
                            tracing::info!("API server exited gracefully");
                        }
                        Err(e) => {
                            tracing::error!("API server exited with error: {e}");
                        }
                    }
                    break api_exit.map_err(Into::into);
                },

                event = state.next() => match event {
                    Some(event) => event,
                    None => break Err(rootcause::report!("Actor event stream closed unexpectedly")),
                }
            };

            match event {
                Event::Signal(signal) => match signal {
                    SignalEvent::GetState(tx) => {
                        let _ = tx.send(DebugState {
                            status: state.status(),
                            uptime: state.uptime().unwrap_or(Duration::default()),
                            description: format!("{:?}", self),
                        });
                    }
                    SignalEvent::GetChildren(tx) => {
                        tx.send(vec![]).ok();
                    }
                    SignalEvent::StatusUpdate(update) => {
                        if update.is_exit() {
                            break Ok(());
                        }
                    }
                },
                Event::Message(_infallible) => unreachable!(),
            }
        }
    }
}

impl ApiServer {
    pub fn new(cfg: ApiConfig, root_supervisor: Pid) -> Self {
        Self {
            root_supervisor,
            cfg: Arc::new(cfg),
        }
    }

    fn create_router(&self) -> AutorouteApiRouter {
        AutorouteApiRouter::new()
            .with_pub_routes(method_routers!(get_tree))
            .with_state(self.clone())
    }

    pub async fn run(self) -> Result<(), Report> {
        let (router, api) = self.create_router().split_for_parts();

        let router = match self.cfg.swagger_ui {
            true => router.merge(
                utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
                    .url("/api-docs/openapi.json", api.clone()),
            ),
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
    State(state): State<ApiServer>,
) -> _ {
    tracing::debug!(
        "Received request for supervision tree with PID: {:?} and query: {:?}",
        pid.pid,
        include_debug.include_debug
    );

    let include_debug = include_debug.include_debug.unwrap_or(false);

    let pid = pid.pid.unwrap_or_else(|| state.root_supervisor.clone());

    let tree = match SupervisionTree::new_populated(ChildDescription {
        pid,
        restart_mode: RestartMode::Always,
        abort_timeout: Duration::from_secs(10),
    })
    .await
    {
        Ok(tree) => tree,
        Err(_) => return "Failed to populate supervision tree".into_404(),
    };

    let tree = if include_debug {
        match tree.with_debug_state().await {
            Ok(tree) => tree,
            Err(_) => return "Failed to populate debug state".into_500(),
        }
    } else {
        tree
    };

    tree.into_ok()
}
