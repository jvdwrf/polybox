use std::time::Duration;

use anyhow::Context;

use crate::_prelude::*;

pub struct Node {
    supervisor: SupervisorBlueprint,
    supervisor_pid: Pid,
    restart_intensity: RestartIntensity,
    shutdown_timeout: Duration,
}

struct NodeActor {
    supervisor_blueprint: SupervisorBlueprint,
    supervisor_child: Child<(), SupervisorInterface>,
    supervisor_pid: Pid,
    restart_limiter: RestartLimiter,
    shutdown_timeout: Duration,
}

impl Node {
    pub fn new(supervisor_pid: impl Into<Pid>, supervisor: SupervisorBlueprint) -> Self {
        Self {
            supervisor,
            supervisor_pid: supervisor_pid.into(),
            restart_intensity: RestartIntensity::new(3, Duration::from_secs(120)),
            shutdown_timeout: Duration::from_secs(30),
        }
    }

    pub fn start(self) -> Result<(), anyhow::Error> {
        let supervisor_child = self.supervisor.spawn(self.supervisor_pid.clone());
        Registry::local()
            .register(supervisor_child.address().clone())
            .context("Root Supervisor failed to register")?;

        let actor = NodeActor {
            supervisor_blueprint: self.supervisor,
            supervisor_child,
            supervisor_pid: self.supervisor_pid,
            restart_limiter: RestartLimiter::new(self.restart_intensity),
            shutdown_timeout: self.shutdown_timeout,
        };

        tokio::task::spawn(actor.run());

        Ok(())
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
                        let new_supervisor_child =
                            self.supervisor_blueprint.spawn(self.supervisor_pid.clone());
                        self.supervisor_child = new_supervisor_child;
                    }
                }
            }
        }
    }

    async fn exit_gracefully(self) -> ! {
        let res = tokio::select! {
            res = self.supervisor_child.shutdown_abort(self.shutdown_timeout) => res,
            _ = wait_for_shutdown_signal() => {
                tracing::error!("Received second shutdown signal during shutdown. Forcing immediate termination.");
                // 130 is standard exit code for SIGINT (128 + signal 2)
                std::process::exit(130);
            }
        };

        match res {
            Ok(()) => {
                tracing::info!("Root-Supervisor exited gracefully. Shutting down node.");
                std::process::exit(0);
            }
            Err(err) => {
                tracing::error!("Root-Supervisor failed to exit gracefully: {:?}", err);
                std::process::exit(1);
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
