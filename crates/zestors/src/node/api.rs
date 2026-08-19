use crate::_prelude::*;
use axum::extract::{Query, State};
use axum_autoroute::{AutorouteApiRouter, autoroute, method_routers};
use smol_str::format_smolstr;
use std::{net::SocketAddr, pin::pin};
use tokio::net::TcpListener;
use utoipa::IntoParams;

#[derive(Clone, Debug)]
pub struct ApiServerBlueprint {
    pub addr: SocketAddr,
}

impl Blueprint for ApiServerBlueprint {
    type Actor = ApiServer;

    fn instantiate(&self) -> Self::Actor {
        ApiServer::new(self.clone())
    }
}

impl ApiServerBlueprint {
    // fn generate_openapi() -> utoipa::openapi::OpenApi {
    //     ApiServer::new(Self::default(), Pid::new(""))
    //         .create_router()
    //         .split_for_parts()
    //         .1
    // }

    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

impl Default for ApiServerBlueprint {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8080".parse().unwrap(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ApiServer {
    pub cfg: Arc<ApiServerBlueprint>,
}

#[derive(Interface)]
#[interface(path = "crate")]
pub enum ApiServerInterface {
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
    pub fn new(cfg: ApiServerBlueprint) -> Self {
        Self { cfg: Arc::new(cfg) }
    }

    fn create_router(&self) -> AutorouteApiRouter {
        AutorouteApiRouter::new()
            .with_pub_routes(method_routers!(get_tree))
            .with_state(self.clone())
    }

    pub async fn run(self) -> Result<(), Report> {
        let (router, api) = self.create_router().split_for_parts();

        let router = router.merge(
            utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
                .url("/api-docs/openapi.json", api.clone()),
        );

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
) -> _ {
    tracing::debug!(
        "Received request for supervision tree with PID: {:?} and query: {:?}",
        pid.pid,
        include_debug.include_debug
    );

    let include_debug = include_debug.include_debug.unwrap_or(false);

    let Some(pid) = pid.pid.or_else(|| Node::root_supervisor_pid().cloned()) else {
        return "No PID provided and no root supervisor PID found".into_500();
    };

    let address = Registry::local().get(&pid);
    if address.is_none() {
        return "PID not found in registry".into_404();
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

    tree.into_ok()
}
