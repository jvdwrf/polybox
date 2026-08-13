use crate::_prelude::*;
use anyhow::Context;
use std::time::Duration;

pub struct Node {
    restart_intensity: RestartIntensity,
    spec: ChildSpec<SupervisorBlueprint>,
}

struct NodeActor {
    supervisor_child: Child<(), SupervisorInterface>,
    restart_limiter: RestartLimiter,
    spec: ChildSpec<SupervisorBlueprint>,
}

impl Node {
    pub fn new(spec: ChildSpec<SupervisorBlueprint>) -> Self {
        Self {
            restart_intensity: RestartIntensity::new(3, Duration::from_secs(120)),
            spec,
        }
    }

    pub fn with_restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = intensity;
        self
    }

    pub fn start(self) -> Result<Address<SupervisorInterface>, anyhow::Error> {
        let supervisor_child = self.spec.spawn();
        let supervisor_address = supervisor_child.address().clone();
        Registry::local()
            .register(self.spec.data.clone())
            .context("Root Supervisor failed to register")?;

        let actor = NodeActor {
            supervisor_child,
            spec: self.spec,
            restart_limiter: RestartLimiter::new(self.restart_intensity),
        };

        tokio::task::spawn(actor.run());

        Ok(supervisor_address)
    }
}

impl NodeActor {
    pub async fn run(mut self) {
        loop {
            let supervisor_exit = tokio::select! {
                res = &mut self.supervisor_child => res,
                _ = wait_for_shutdown_signal() => {
                    tracing::info!("Received Ctrl+C signal. Shutting down node.");
                    self.exit_gracefully().await;
                }
            };

            match supervisor_exit {
                Ok(()) => {
                    tracing::info!("Root-Supervisor exited gracefully. Shutting down node.");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(0);
                }
                Err(err) => {
                    if !self.restart_limiter.allow_restart() {
                        tracing::error!("Root-Supervisor exited with error: {:?}", err);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        std::process::exit(1);
                    } else {
                        tracing::warn!(
                            "Root-Supervisor exited with error: {:?}. Restarting...",
                            err
                        );
                        let new_supervisor_child = self.spec.spawn();
                        self.supervisor_child = new_supervisor_child;
                    }
                }
            }
        }
    }

    async fn exit_gracefully(self) -> ! {
        let timeout = self.spec.abort_timeout;

        tokio::select! {
            exit = self.supervisor_child.shutdown_abort(timeout) => {
                match exit {
                    Ok(()) => {
                        tracing::info!("Root-Supervisor exited gracefully. Shutting down node.");
                        std::process::exit(0);
                    }
                    Err(err) => {
                        tracing::error!("Root-Supervisor failed to exit gracefully within timeout: {:?}", err);
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        std::process::exit(1);
                    }
                }
            }
            _ = wait_for_shutdown_signal() => {
                tracing::warn!("Received second shutdown signal. Forcing immediate termination.");
                std::process::exit(130);
            }
        }
    }
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received Ctrl+C (SIGINT) signal."),
        _ = terminate => tracing::info!("Received SIGTERM signal."),
    }
}
