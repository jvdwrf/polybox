use rootcause::Report;
use smol_str::format_smolstr;
use std::{net::SocketAddr, pin::pin, sync::Arc};
use tokio::net::TcpListener;
use zestors_core::{
    prelude::*,
    supervision::{GetChildren, GetDebugInfo, GetHealth, HealthStatus},
};

mod router;

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
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

#[derive(Clone, Debug)]
pub struct ApiServer {
    cfg: Arc<ApiServerBlueprint>,
}

#[derive(Interface)]
#[interface(path = "zestors_core")]
pub enum ApiServerInterface {
    Debug(Payload<GetDebugInfo>),
    Children(Payload<GetChildren>),
    Health(Payload<GetHealth>),
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
                    ApiServerInterface::Health((_, tx)) => {
                        tx.send(HealthStatus::Healthy).ok();
                    }
                },
            }
        }
    }
}

impl ApiServer {
    fn new(cfg: ApiServerBlueprint) -> Self {
        Self { cfg: Arc::new(cfg) }
    }

    pub fn blueprint(addr: SocketAddr) -> ApiServerBlueprint {
        ApiServerBlueprint::new(addr)
    }

    async fn run(self) -> Result<(), Report> {
        let router = self.create_router();

        let listener = TcpListener::bind(self.cfg.addr).await?;

        tracing::info!("API server running at http://{}", self.cfg.addr);

        axum::serve(listener, router.into_make_service()).await?;

        Ok(())
    }
}
