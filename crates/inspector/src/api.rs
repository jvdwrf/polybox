use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::mpsc;
use zestors::supervision::SupervisionTree;

use crate::client::Client;

static CLIENT: LazyLock<Client> =
    LazyLock::new(|| Client::new("http://localhost:8080").expect("Failed to create API client"));

const SLEEP_INTERVAL: Duration = Duration::from_millis(500);

pub enum ApiMessage {
    NewTree(rootcause::Result<Option<SupervisionTree>>),
}

pub async fn run_tree_poller(sender: mpsc::Sender<ApiMessage>, ctx: egui::Context) {
    loop {
        let tree = CLIENT.get_tree().await;

        sender.send(ApiMessage::NewTree(tree)).await.ok();

        ctx.request_repaint();
        tokio::time::sleep(SLEEP_INTERVAL).await;
    }
}
