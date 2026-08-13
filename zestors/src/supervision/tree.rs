use crate::_prelude::*;
use std::collections::VecDeque;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SupervisionTree {
    pid: Pid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    debug_state: Option<DebugState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(no_recursion)]
    children: Vec<SupervisionTree>,
}

impl SupervisionTree {
    pub fn new(pid: Pid) -> Self {
        Self {
            pid,
            debug_state: None,
            children: Vec::new(),
        }
    }

    pub async fn new_populated(pid: Pid) -> Result<Self, anyhow::Error> {
        let mut tree = Self::new(pid);
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
            let address = Registry::local()
                .get(&node.pid)
                .ok_or_else(|| anyhow::anyhow!("Failed to get address from Registry"))?;

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
