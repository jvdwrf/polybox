use crate::{_prelude::*, schemas::DurationSchema};
use std::collections::VecDeque;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SupervisionTree {
    pid: Pid,

    restart_mode: RestartMode,

    #[schema(value_type = DurationSchema)]
    #[serde(with = "DurationSchema")]
    abort_timeout: Duration,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    debug_state: Option<DebugState>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(no_recursion)]
    children: Vec<SupervisionTree>,
}

impl SupervisionTree {
    pub fn new(child: ChildDescription) -> Self {
        Self {
            pid: child.pid,
            debug_state: None,
            restart_mode: child.restart_mode,
            abort_timeout: child.abort_timeout,
            children: Vec::new(),
        }
    }

    pub async fn new_populated(child: ChildDescription) -> Self {
        let mut tree = Self::new(child);
        tree.populate_tree().await;
        tree
    }

    async fn populate_children(&mut self) {
        let Some(address) = Registry::local().get(&self.pid) else {
            return;
        };

        let children = match address.get_children().await {
            Ok(children) => children,
            Err(err) => {
                tracing::warn!("Failed to get children for PID {}: {}", self.pid, err);
                vec![]
            }
        };

        for child in children {
            self.children.push(SupervisionTree::new(child));
        }
    }

    pub async fn populate_tree(&mut self) -> () {
        let mut queue = VecDeque::new();
        queue.push_back(self);

        while let Some(node) = queue.pop_front() {
            node.populate_children().await;

            for child in &mut node.children {
                queue.push_back(child);
            }
        }
    }

    pub async fn populate_debug_state(&mut self) {
        let mut queue = VecDeque::new();
        queue.push_back(self);

        while let Some(node) = queue.pop_front() {
            let address = match Registry::local().get(&node.pid) {
                Some(address) => address,
                None => {
                    continue;
                }
            };

            let Ok(debug_state) = address.get_debug_state().await else {
                continue;
            };
            node.debug_state = Some(debug_state);

            for child in &mut node.children {
                queue.push_back(child);
            }
        }
    }

    pub async fn with_debug_state(mut self) -> Self {
        self.populate_debug_state().await;
        self
    }
}
