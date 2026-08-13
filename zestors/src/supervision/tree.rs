use std::collections::VecDeque;

use crate::_prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SupervisionTree {
    pid: Pid,
    children: Vec<SupervisionTree>,
}

impl SupervisionTree {
    pub fn new(pid: Pid) -> Self {
        Self {
            pid,
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

        let children = address.get_children().await?;

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
}
