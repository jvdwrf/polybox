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

    eframe::run_native(
        "Zestors Inspector",
        options,
        Box::new(|cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            tokio::spawn(update_supervision_tree_background_process(
                app.sender.clone(),
                cc.egui_ctx.clone(),
            ));
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

            if let Some(tree_result) = &self.tree {
                match tree_result {
                    Ok(tree) => {
                        // Scrollable in both directions to prevent window overflow
                        egui::ScrollArea::both()
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                render_supervision_node(ui, tree, true);
                            });
                    }
                    Err(e) => {
                        ui.colored_label(
                            egui::Color32::RED,
                            format!("Error loading tree: {:#?}", e),
                        );
                    }
                }
            } else {
                ui.label("No tree data yet.");
            }
        });
    }
}

async fn update_supervision_tree_background_process(
    sender: mpsc::Sender<MyMessage>,
    ctx: egui::Context,
) {
    loop {
        let tree = default_api::get_tree(&CFG, Some(true), None).await;
        sender
            .send(MyMessage::NewTree(tree.map_err(Into::into)))
            .await
            .ok();
        ctx.request_repaint();
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

use egui::{Color32, CornerRadius, Margin, Stroke};

/// Recursively renders a single SupervisionTree node and its children.
fn render_supervision_node(ui: &mut egui::Ui, node: &models::SupervisionTree, default_open: bool) {
    let status_badge = match &node.debug_state {
        Some(Some(debug)) => format!(" [{:?}]", debug.status),
        _ => String::new(),
    };

    let title = format!("PID: {}{}", node.pid, status_badge);

    // Frame wrapper to draw explicit visual borders around each tree node
    egui::Frame::group(ui.style())
        .fill(ui.visuals().window_fill().linear_multiply(0.3)) // Subtle card fill
        .stroke(Stroke::new(
            1.0,
            ui.visuals().widgets.noninteractive.bg_stroke.color,
        ))
        .corner_radius(CornerRadius::same(6))
        .inner_margin(Margin::same(8))
        .show(ui, |ui| {
            egui::CollapsingHeader::new(title)
                .id_salt(ui.make_persistent_id(&node.pid))
                .default_open(default_open)
                .show(ui, |ui| {
                    ui.add_space(4.0);

                    // Structured Key-Value Grid for Node Metadata
                    egui::Grid::new(ui.make_persistent_id(&format!("{}_grid", node.pid)))
                        .num_columns(2)
                        .spacing([12.0, 4.0])
                        .show(ui, |ui| {
                            ui.label("Restart Mode:");
                            ui.code(format!("{:?}", node.restart_mode));
                            ui.end_row();

                            ui.label("Abort Timeout:");
                            ui.code(format_duration(&node.abort_timeout));
                            ui.end_row();
                        });

                    // Debug State Section
                    if let Some(Some(debug)) = &node.debug_state {
                        ui.add_space(6.0);

                        // Distinct inner frame for Debug State to isolate it visually
                        egui::Frame::canvas(ui.style())
                            .stroke(Stroke::new(
                                1.0,
                                ui.visuals().widgets.noninteractive.bg_stroke.color,
                            ))
                            .corner_radius(CornerRadius::same(4))
                            .inner_margin(Margin::same(6))
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new("Debug State").strong().small());
                                ui.add_space(2.0);

                                egui::Grid::new(
                                    ui.make_persistent_id(&format!("{}_debug_grid", node.pid)),
                                )
                                .num_columns(2)
                                .spacing([12.0, 4.0])
                                .show(ui, |ui| {
                                    ui.label("Status:");
                                    ui.code(format!("{:?}", debug.status));
                                    ui.end_row();

                                    ui.label("Uptime:");
                                    ui.code(format_duration(&debug.uptime));
                                    ui.end_row();

                                    ui.label("Description:");
                                    // Label wrapping prevents horizontal overflow out of parent container
                                    ui.add(egui::Label::new(&debug.description).wrap());
                                    ui.end_row();
                                });
                            });
                    }

                    // Recursive Children
                    if let Some(children) = &node.children {
                        if !children.is_empty() {
                            ui.add_space(8.0);
                            ui.vertical(|ui| {
                                for child in children {
                                    render_supervision_node(ui, child, false);
                                    ui.add_space(4.0);
                                }
                            });
                        }
                    }
                });
        });
}

/// Human-readable duration formatter that dynamically handles fractional units.
fn format_duration(d: &models::DurationSchema) -> String {
    let total_secs = d.secs;
    let nanos = d.nanos;

    if total_secs == 0 && nanos == 0 {
        return "0s".to_string();
    }

    if total_secs >= 86400 {
        format!("{}d {}h", total_secs / 86400, (total_secs % 86400) / 3600)
    } else if total_secs >= 3600 {
        format!("{}h {}m", total_secs / 3600, (total_secs % 3600) / 60)
    } else if total_secs >= 60 {
        format!("{}m {}s", total_secs / 60, total_secs % 60)
    } else if total_secs > 0 {
        let ms = nanos as f64 / 1_000_000.0;
        if ms > 0.0 {
            format!("{}.{:03}s", total_secs, (nanos / 1_000_000))
        } else {
            format!("{}s", total_secs)
        }
    } else if nanos >= 1_000_000 {
        format!("{:.2}ms", nanos as f64 / 1_000_000.0)
    } else if nanos >= 1_000 {
        format!("{:.2}µs", nanos as f64 / 1_000.0)
    } else {
        format!("{}ns", nanos)
    }
}
