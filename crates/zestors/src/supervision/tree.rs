use crate::_prelude::*;
use std::collections::VecDeque;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SupervisionTree {
    pid: Pid,

    restart_mode: RestartMode,
    #[schema(value_type = String, example = "2024-06-01T12:00:00Z")]
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

    pub async fn new_populated(child: ChildDescription) -> Result<Self, anyhow::Error> {
        let mut tree = Self::new(child);
        tree.populate_tree().await?;
        Ok(tree)
    }

    async fn populate_children(&mut self) -> Result<(), anyhow::Error> {
        let address = Registry::local()
            .get(&self.pid)
            .ok_or_else(|| anyhow::anyhow!("Failed to get address from Registry"))?;

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

        Ok(())
    }

    pub async fn populate_tree(&mut self) -> Result<(), anyhow::Error> {
        let mut queue = VecDeque::new();
        queue.push_back(self);

        while let Some(node) = queue.pop_front() {
            node.populate_children().await?;

            for child in &mut node.children {
                queue.push_back(child);
            }
        }

        Ok(())
    }

    pub async fn populate_debug_state(&mut self) -> Result<(), anyhow::Error> {
        let mut queue = VecDeque::new();
        queue.push_back(self);

        while let Some(node) = queue.pop_front() {
            let entry = Registry::local()
                .get_entry(&node.pid)
                .ok_or_else(|| anyhow::anyhow!("Failed to get address from Registry"))?;

            let address = entry.address();

            let debug_state = address.get_debug_state().await?;
            node.debug_state = Some(debug_state);

            for child in &mut node.children {
                queue.push_back(child);
            }
        }

        Ok(())
    }

    pub async fn with_debug_state(mut self) -> Result<Self, anyhow::Error> {
        self.populate_debug_state().await?;
        Ok(self)
    }
}
