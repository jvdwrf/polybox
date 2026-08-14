#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use egui::{Color32, CornerRadius, Margin, RichText, Stroke, collapsing_header::CollapsingState};
use std::sync::LazyLock;
use tokio::sync::mpsc::{self};
use zestors_api_client::{
    apis::{configuration, default_api},
    models,
};

// --- Palette Definition ---
const BG_DARK: Color32 = Color32::from_rgb(18, 20, 26);
const CARD_BG: Color32 = Color32::from_rgb(26, 29, 38);
const INNER_CARD_BG: Color32 = Color32::from_rgb(20, 22, 30);
const BORDER_COLOR: Color32 = Color32::from_rgb(45, 50, 66);
const PID_BLUE: Color32 = Color32::from_rgb(97, 175, 239);
const LABEL_MUTED: Color32 = Color32::from_rgb(140, 148, 170);
const VALUE_PURPLE: Color32 = Color32::from_rgb(198, 120, 221);

static CFG: LazyLock<configuration::Configuration> =
    LazyLock::new(|| configuration::Configuration {
        base_path: "http://localhost:8080".to_owned(),
        ..Default::default()
    });

fn main() -> eframe::Result {
    env_logger::init();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    std::thread::spawn(move || {
        rt.block_on(futures::future::pending::<()>());
    });

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([500.0, 600.0]),
        ..Default::default()
    };

    let app = MyApp::default();

    eframe::run_native(
        "Zestors Inspector",
        options,
        Box::new(|cc| {
            let mut visuals = egui::Visuals::dark();
            visuals.panel_fill = BG_DARK;
            visuals.window_fill = BG_DARK;
            visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, BORDER_COLOR);
            cc.egui_ctx.set_visuals(visuals);

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
        while let Ok(msg) = self.receiver.try_recv() {
            match msg {
                MyMessage::NewTree(supervision_tree) => {
                    self.tree = Some(supervision_tree);
                }
            }
        }

        egui::CentralPanel::default().show(ui, |ui| {
            ui.add_space(4.0);
            ui.heading(
                RichText::new("⚡ Zestors Inspector")
                    .strong()
                    .color(Color32::from_rgb(220, 225, 240)),
            );
            ui.add_space(4.0);
            ui.separator();
            ui.add_space(4.0);

            if let Some(tree_result) = &self.tree {
                match tree_result {
                    Ok(tree) => {
                        egui::ScrollArea::both()
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                render_supervision_node(ui, tree, true);
                            });
                    }
                    Err(e) => {
                        ui.colored_label(
                            Color32::from_rgb(224, 108, 117),
                            format!("Error loading tree: {:#?}", e),
                        );
                    }
                }
            } else {
                ui.label(RichText::new("No tree data yet...").color(LABEL_MUTED));
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

/// Recursively renders a single SupervisionTree node with modern card styling.
fn render_supervision_node(ui: &mut egui::Ui, node: &models::SupervisionTree, default_open: bool) {
    let node_id = ui.make_persistent_id(&node.pid);

    egui::Frame::group(ui.style())
        .fill(CARD_BG)
        .stroke(Stroke::new(1.0, BORDER_COLOR))
        .corner_radius(CornerRadius::same(6))
        .inner_margin(Margin::same(8))
        .show(ui, |ui| {
            // CollapsingState allows custom interactive UI in the header row
            CollapsingState::load_with_default_open(ui.ctx(), node_id, default_open)
                .show_header(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("PID:").small().color(LABEL_MUTED));
                        ui.label(RichText::new(&node.pid).strong().color(PID_BLUE));

                        if let Some(Some(debug)) = &node.debug_state {
                            let status_str = format!("{:?}", debug.status);
                            let (badge_bg, badge_fg) = get_status_colors(&status_str);

                            egui::Frame::canvas(ui.style())
                                .fill(badge_bg)
                                .corner_radius(CornerRadius::same(4))
                                .inner_margin(Margin::symmetric(6, 2))
                                .show(ui, |ui| {
                                    ui.label(
                                        RichText::new(status_str).small().strong().color(badge_fg),
                                    );
                                });
                        }
                    });
                })
                .body(|ui| {
                    ui.add_space(4.0);

                    // Structured Key-Value Grid
                    egui::Grid::new(ui.make_persistent_id(&format!("{}_grid", node.pid)))
                        .num_columns(2)
                        .spacing([12.0, 6.0])
                        .show(ui, |ui| {
                            ui.label(RichText::new("Restart Mode:").color(LABEL_MUTED));
                            ui.label(
                                RichText::new(format!("{:?}", node.restart_mode))
                                    .color(VALUE_PURPLE),
                            );
                            ui.end_row();

                            ui.label(RichText::new("Abort Timeout:").color(LABEL_MUTED));
                            ui.label(
                                RichText::new(format_duration(&node.abort_timeout))
                                    .color(Color32::WHITE),
                            );
                            ui.end_row();
                        });

                    // Debug State Container
                    if let Some(Some(debug)) = &node.debug_state {
                        ui.add_space(6.0);

                        egui::Frame::canvas(ui.style())
                            .fill(INNER_CARD_BG)
                            .stroke(Stroke::new(1.0, BORDER_COLOR))
                            .corner_radius(CornerRadius::same(4))
                            .inner_margin(Margin::same(8))
                            .show(ui, |ui| {
                                ui.label(
                                    RichText::new("DEBUG STATE")
                                        .small()
                                        .strong()
                                        .color(LABEL_MUTED),
                                );
                                ui.add_space(4.0);

                                egui::Grid::new(
                                    ui.make_persistent_id(&format!("{}_debug_grid", node.pid)),
                                )
                                .num_columns(2)
                                .spacing([12.0, 4.0])
                                .show(ui, |ui| {
                                    ui.label(RichText::new("Uptime:").color(LABEL_MUTED));
                                    ui.label(
                                        RichText::new(format_duration(&debug.uptime))
                                            .color(Color32::WHITE),
                                    );
                                    ui.end_row();

                                    ui.label(RichText::new("Description:").color(LABEL_MUTED));
                                    ui.add(
                                        egui::Label::new(
                                            RichText::new(&debug.description)
                                                .color(Color32::LIGHT_GRAY),
                                        )
                                        .wrap(),
                                    );
                                    ui.end_row();
                                });
                            });
                    }

                    // Render Children
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

/// Dynamic Status Badge Colors (Background, Foreground Text)
fn get_status_colors(status: &str) -> (Color32, Color32) {
    let s = status.to_lowercase();
    if s.contains("running") || s.contains("active") || s.contains("ok") {
        (
            Color32::from_rgb(34, 60, 45),
            Color32::from_rgb(152, 195, 121),
        )
    } else if s.contains("stop") || s.contains("fail") || s.contains("error") {
        (
            Color32::from_rgb(65, 35, 40),
            Color32::from_rgb(224, 108, 117),
        )
    } else if s.contains("restart") || s.contains("init") {
        (
            Color32::from_rgb(60, 50, 30),
            Color32::from_rgb(229, 192, 123),
        )
    } else {
        (
            Color32::from_rgb(38, 45, 60),
            Color32::from_rgb(97, 175, 239),
        )
    }
}

/// Human-readable duration formatter
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
        format!("{}.{:03}s", total_secs, nanos / 1_000_000)
    } else if nanos >= 1_000_000 {
        format!("{:.2}ms", nanos as f64 / 1_000_000.0)
    } else if nanos >= 1_000 {
        format!("{:.2}µs", nanos as f64 / 1_000.0)
    } else {
        format!("{}ns", nanos)
    }
}
