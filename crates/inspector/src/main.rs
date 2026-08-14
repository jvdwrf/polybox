#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use std::sync::LazyLock;

use eframe::egui;
use tokio::sync::mpsc::{self};
use zestors_api_client::{
    apis::{configuration, default_api},
    models,
};

static CFG: LazyLock<configuration::Configuration> =
    LazyLock::new(|| configuration::Configuration {
        base_path: "http://localhost:8080".to_owned(),
        ..Default::default()
    });

fn main() -> eframe::Result {
    env_logger::init();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    // Execute the runtime in its own thread. // The future doesn't have to do anything. In this example, it just sleeps forever.
    std::thread::spawn(move || {
        rt.block_on(futures::future::pending::<()>());
    });

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([320.0, 240.0]),
        ..Default::default()
    };

    let app = MyApp::default();

    tokio::spawn(update_supervision_tree_background_process(
        app.sender.clone(),
    ));

    eframe::run_native(
        "Zestors Inspector",
        options,
        Box::new(|cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            Ok(Box::new(app))
        }),
    )
}

enum MyMessage {
    NewTree(rootcause::Result<models::SupervisionTree>),
}

struct MyApp {
    sender: mpsc::Sender<MyMessage>,
    receiver: mpsc::Receiver<MyMessage>,
    tree: Option<rootcause::Result<models::SupervisionTree>>,
}

impl Default for MyApp {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(1_000_000);
        Self {
            sender: tx,
            receiver: rx,
            tree: None,
        }
    }
}

impl eframe::App for MyApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // On each frame, handle messages from the channel.
        while let Ok(msg) = self.receiver.try_recv() {
            match msg {
                MyMessage::NewTree(supervision_tree) => {
                    self.tree = Some(supervision_tree);
                }
            }
        }

        egui::CentralPanel::default().show(ui, |ui| {
            ui.heading("Zestors Inspector");

            ui.separator();

            ui.horizontal(|ui| {
                if let Some(tree) = &self.tree {
                    match tree {
                        Ok(tree) => {
                            ui.label(format!("Tree: {:#?}", tree));
                        }
                        Err(e) => {
                            eprintln!("Error: {:#?}", e);
                            ui.label(format!("Error: {:#?}", e));
                        }
                    }
                } else {
                    ui.label("No tree data yet.");
                }
            });
        });
    }
}

async fn update_supervision_tree_background_process(sender: mpsc::Sender<MyMessage>) {
    loop {
        let tree = default_api::get_tree(&CFG, Some(true), None).await;
        sender
            .send(MyMessage::NewTree(tree.map_err(Into::into)))
            .await
            .ok();
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}
