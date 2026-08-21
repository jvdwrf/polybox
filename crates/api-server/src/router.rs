use std::time::Duration;

use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use axum_typed_routing::{TypedRouter, route};
use futures::{StreamExt, stream};
use indexmap::IndexMap;
use rootcause::report;
use zestors_core::{
    channel::ChannelSnapshot,
    node::Node,
    prelude::*,
    registry::Registry,
    signals::{DebugInfo, RestartMode},
    supervision::{ChildConfig, ChildDescription, GetChildren, GetDebugInfo, SupervisionTree},
};

impl ApiServer {
    pub(super) fn create_router(&self) -> Router {
        Router::new()
            .typed_route(get_tree)
            .typed_route(get_processes)
            .typed_route(get_channel_snapshots)
            .typed_route(get_debug_info)
            .with_state(self.clone())
    }
}

#[route(GET "/tree?pid&include_debug" with ApiServer)]
async fn get_tree(pid: Option<Pid>, include_debug: Option<bool>) -> ApiResult {
    tracing::debug!("Received request for supervision with {pid:?} and {include_debug:?}");

    let include_debug = include_debug.unwrap_or(false);

    let pid = pid
        .or_else(|| Node::root_supervisor().map(|desc| desc.pid.clone()))
        .ok_or_else(|| report!("No PID provided and no root supervisor PID found"))?;

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

    Ok((StatusCode::OK, Json(tree)).into_response())
}

#[route(GET "/processes" with ApiServer)]
async fn get_processes() -> ApiResult<Json<Vec<ChildDescription>>> {
    let root_desc = Node::root_supervisor()
        .ok_or_else(|| report!("No root supervisor"))?
        .clone();

    let mut pending = Vec::from_iter([root_desc.pid.clone()]);
    let mut found = IndexMap::<Pid, _>::from_iter([(root_desc.pid.clone(), root_desc)]);

    while !pending.is_empty() {
        let new_children = stream::iter(pending.drain(..))
            .map(|pid| async move {
                let Some(address) = Registry::local().get(&pid) else {
                    return None;
                };

                match timeout(Duration::from_millis(50), address.request_dyn(GetChildren)).await {
                    Ok(Ok(children)) => Some(children),
                    _ => None,
                }
            })
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await;

        for child in new_children.into_iter().flatten().flatten() {
            let child_pid = child.pid.clone();

            if found.insert(child_pid.clone(), child).is_none() {
                pending.push(child_pid);
            }
        }
    }

    Ok(Json(found.into_values().collect()))
}

#[route(GET "/snapshots" with ApiServer)]
async fn get_channel_snapshots(
    Json(pids): Json<Vec<Pid>>,
) -> ApiResult<Json<Vec<Option<ChannelSnapshot>>>> {
    let results = pids
        .into_iter()
        .map(|pid| {
            Registry::local()
                .get(&pid)
                .map(|address| address.snapshot())
        })
        .collect::<Vec<_>>();

    Ok(Json(results))
}

#[route(GET "/debug_info" with ApiServer)]
async fn get_debug_info(Json(pids): Json<Vec<Pid>>) -> ApiResult<Json<Vec<Option<DebugInfo>>>> {
    let results = stream::iter(pids.into_iter().map(|pid| async move {
        let Some(address) = Registry::local().get(&pid) else {
            return None;
        };

        match timeout(Duration::from_millis(50), address.request_dyn(GetDebugInfo)).await {
            Ok(Ok(debug_info)) => Some(debug_info),
            _ => None,
        }
    }))
    .buffered(10)
    .collect::<Vec<_>>()
    .await;

    Ok(Json(results))
}

use error::*;
use tokio::time::timeout;

use crate::ApiServer;
mod error;
