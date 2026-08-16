use crate::api::ApiMessage;
use crate::components::SupervisionNodeWidget;
use crate::theme::Theme;
use egui::{Color32, RichText};
use tokio::sync::mpsc;
use zestors_api_client::types;

pub struct MyApp {
    pub sender: mpsc::Sender<ApiMessage>,
    pub receiver: mpsc::Receiver<ApiMessage>,
    pub tree: Option<rootcause::Result<types::SupervisionTree>>,
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

impl MyApp {
    fn handle_incoming_messages(&mut self) {
        while let Ok(msg) = self.receiver.try_recv() {
            match msg {
                ApiMessage::NewTree(supervision_tree) => {
                    self.tree = Some(supervision_tree);
                }
            }
        }
    }

    fn render_header(&self, ui: &mut egui::Ui) {
        ui.add_space(4.0);
        ui.heading(
            RichText::new("⚡ Zestors Inspector")
                .strong()
                .color(Color32::from_rgb(220, 225, 240)),
        );
        ui.add_space(4.0);
        ui.separator();
        ui.add_space(4.0);
    }
}

impl eframe::App for MyApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.handle_incoming_messages();

        egui::CentralPanel::default().show(ui, |ui| {
            self.render_header(ui);

            if let Some(tree_result) = &self.tree {
                match tree_result {
                    Ok(tree) => {
                        egui::ScrollArea::both()
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                SupervisionNodeWidget::new(tree, true).show(ui);
                            });
                    }
                    Err(e) => {
                        ui.colored_label(Theme::ERROR_RED, format!("Error loading tree: {:#?}", e));
                    }
                }
            } else {
                ui.label(RichText::new("No tree data yet...").color(Theme::LABEL_MUTED));
            }
        });
    }
}
