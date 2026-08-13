use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use std::{marker::PhantomData, net::SocketAddr};
use tokio::net::TcpListener;

use crate::_prelude::*;

pub struct ApiConfig {
    pub socket_addr: SocketAddr,
    pub root_supervisor: Address<SupervisorInterface>,
}

impl ApiConfig {
    async fn run_api(self) -> Result<(), anyhow::Error> {
        let cfg = Arc::new(self);

        let app = Router::new()
            .route("/tree/:pid", get(get_tree))
            .with_state(cfg.clone());

        let listener = TcpListener::bind(cfg.socket_addr).await?;

        axum::serve(listener, app.into_make_service()).await?;

        Ok(())
    }
}

async fn get_tree(
    State(state): State<Arc<ApiConfig>>,
    Path(pid): Path<Option<Pid>>,
) -> Result<Json<SupervisionTree>, StatusCode> {
    // let address = match pid {
    //     Some(pid) => {
    //         Registry::local().get(&pid).ok_or(StatusCode::NOT_FOUND)? // Returns 404 if None
    //     }
    //     None => state.root_supervisor.clone().into_dyn(),
    // };

    // let tree = SupervisionTree::new_populated(Pid::rand())
    //     // .await
    //     // .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    //     // .with_debug_state()
    //     .await
    //     .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Ok(Json(tree))

    let inbox: DynInbox<Set!()> = inbox();
    let addr: Address<Set!()> = address();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    todo!()
}

fn address() -> Address<Set![]> {
    todo!()
}

fn inbox() -> DynInbox<Set![]> {
    todo!()
}

struct Test<T>(PhantomData<fn() -> T>);

impl<T> Test<T> {
    fn new() -> Self {
        Self(PhantomData)
    }
}
