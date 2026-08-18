use crate::_prelude::*;
use axum::extract::{Query, State};
use axum_autoroute::{AutorouteApiRouter, autoroute, method_routers};
use smol_str::format_smolstr;
use std::{net::SocketAddr, pin::pin};
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

#[derive(Interface)]
#[interface(crate = "crate")]
pub(super) enum ApiServerInterface {
    Debug(Payload<GetDebug>),
    Children(Payload<GetChildren>),
}

impl Actor for ApiServer {
    type Interface = ApiServerInterface;
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
                    SignalEvent::Shutdown => {
                        tracing::info!("API server received shutdown signal");
                        break Ok(());
                    }
                    SignalEvent::Resume | SignalEvent::Suspend => {}
                },

                Event::Message(msg) => match msg {
                    ApiServerInterface::Debug((_, tx)) => {
                        let _ = tx.send(format_smolstr!("{self:?}").into());
                    }

                    ApiServerInterface::Children((_, tx)) => {
                        tx.send(vec![]).ok();
                    }
                },
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

    let address = Registry::local().get(&pid);
    if address.is_none() {
        return "PID not found in registry".into_404();
    }

    let tree = SupervisionTree::new(ChildDescription {
        pid,
        cfg: SuperviseeConfig {
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

    tree.into_ok()
}
