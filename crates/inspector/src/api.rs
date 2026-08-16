use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::mpsc;
use zestors_api_client::{
    client::{self, default::parameters},
    types,
};

static CLIENT: LazyLock<client::Client> = LazyLock::new(|| {
    client::Client::new("http://localhost:8080").expect("Failed to create API client")
});

const SLEEP_INTERVAL: Duration = Duration::from_millis(500);

pub enum ApiMessage {
    NewTree(rootcause::Result<types::SupervisionTree>),
}

pub async fn run_tree_poller(sender: mpsc::Sender<ApiMessage>, ctx: egui::Context) {
    loop {
        let tree = CLIENT
            .get_tree(&parameters::GetTreeQuery {
                include_debug: Some(true),
                pid: None,
            })
            .await;

        sender
            .send(ApiMessage::NewTree(tree.map_err(Into::into)))
            .await
            .ok();

        ctx.request_repaint();
        tokio::time::sleep(SLEEP_INTERVAL).await;
    }
}
