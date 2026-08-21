use super::debug_card::render_debug_card;
use super::status_badge::render_actor_status_badge;
use crate::{app::ProcessTree, theme::Theme, utils::format_duration};
use egui::{CornerRadius, Frame, Margin, RichText, Stroke, Ui, collapsing_header::CollapsingState};

pub struct SupervisionNodeWidget<'a> {
    tree: &'a ProcessTree<'a>,
    default_open: bool,
}

impl<'a> SupervisionNodeWidget<'a> {
    pub fn new(tree: &'a ProcessTree<'a>, default_open: bool) -> Self {
        Self { tree, default_open }
    }

    pub fn show(self, ui: &mut Ui) {
        let process = self.tree.entry;
        let node_id = ui.make_persistent_id(("process", &process.pid));

        let is_recently_outdated = process.outdated_since.is_some();

        let text_color = if is_recently_outdated {
            Theme::LABEL_MUTED
        } else {
            egui::Color32::WHITE
        };

        Frame::group(ui.style())
            .fill(Theme::CARD_BG)
            .stroke(Stroke::new(1.0, Theme::BORDER_COLOR))
            .corner_radius(CornerRadius::same(6))
            .inner_margin(Margin::same(8))
            .show(ui, |ui| {
                ui.visuals_mut().override_text_color = Some(text_color);

                CollapsingState::load_with_default_open(ui.ctx(), node_id, self.default_open)
                    .show_header(ui, |ui| {
                        self.render_header(ui, is_recently_outdated);
                    })
                    .body(|ui| {
                        self.render_body(ui);
                    });
            });
    }

    fn render_header(&self, ui: &mut Ui, is_recently_outdated: bool) {
        let process = self.tree.entry;

        ui.horizontal(|ui| {
            ui.label(
                RichText::new("PID:")
                    .small()
                    .color(if is_recently_outdated {
                        Theme::LABEL_MUTED
                    } else {
                        Theme::LABEL_MUTED
                    }),
            );

            ui.label(
                RichText::new(&process.pid)
                    .strong()
                    .color(if is_recently_outdated {
                        Theme::LABEL_MUTED
                    } else {
                        Theme::PID_BLUE
                    }),
            );

            render_actor_status_badge(ui, &process.status);

            if !self.tree.children.is_empty() {
                ui.label(
                    RichText::new(format!("• {} children", self.tree.children.len()))
                        .small()
                        .color(Theme::LABEL_MUTED),
                );
            }
        });
    }

    fn render_body(&self, ui: &mut Ui) {
        let process = self.tree.entry;

        // Process configuration
        self.render_config(ui);

        // Snapshot / Debug details
        if process.snapshot.is_some() || process.debug.is_some() {
            ui.add_space(8.0);

            let details_id = ui.make_persistent_id(("process-details", &process.pid));

            CollapsingState::load_with_default_open(ui.ctx(), details_id, false)
                .show_header(ui, |ui| {
                    ui.label(RichText::new("Details").strong().color(Theme::LABEL_MUTED));
                })
                .body(|ui| {
                    self.render_details(ui);
                });
        }

        // Children
        if !self.tree.children.is_empty() {
            ui.add_space(8.0);

            let children_id = ui.make_persistent_id(("process-children", &process.pid));

            CollapsingState::load_with_default_open(ui.ctx(), children_id, true)
                .show_header(ui, |ui| {
                    ui.label(
                        RichText::new(format!("Children ({})", self.tree.children.len()))
                            .strong()
                            .color(Theme::LABEL_MUTED),
                    );
                })
                .body(|ui| {
                    ui.add_space(4.0);

                    ui.vertical(|ui| {
                        for child in &self.tree.children {
                            SupervisionNodeWidget::new(child, false).show(ui);
                            ui.add_space(4.0);
                        }
                    });
                });
        }
    }

    fn render_config(&self, ui: &mut Ui) {
        let cfg = &self.tree.entry.cfg;

        egui::Grid::new(ui.make_persistent_id(("process-config", &self.tree.entry.pid)))
            .num_columns(2)
            .spacing([12.0, 6.0])
            .show(ui, |ui| {
                ui.label(RichText::new("Restart Mode:").color(Theme::LABEL_MUTED));

                ui.label(
                    RichText::new(format!("{:?}", cfg.restart_mode)).color(Theme::VALUE_PURPLE),
                );

                ui.end_row();

                ui.label(RichText::new("Abort Timeout:").color(Theme::LABEL_MUTED));

                ui.label(
                    RichText::new(format_duration(&cfg.abort_timeout)).color(egui::Color32::WHITE),
                );

                ui.end_row();

                ui.label(RichText::new("Init Timeout:").color(Theme::LABEL_MUTED));

                ui.label(
                    RichText::new(format_duration(&cfg.init_timeout)).color(egui::Color32::WHITE),
                );

                ui.end_row();
            });
    }

    fn render_details(&self, ui: &mut Ui) {
        let process = self.tree.entry;

        if let Some(snapshot) = &process.snapshot {
            ui.label(
                RichText::new("Channel Snapshot")
                    .strong()
                    .color(Theme::LABEL_MUTED),
            );

            // Render snapshot here.
            //
            // Replace this with your actual ChannelSnapshot rendering.
            ui.label(format!("{snapshot:?}"));
        }

        if let Some(debug) = &process.debug {
            if process.snapshot.is_some() {
                ui.add_space(8.0);
            }

            render_debug_card(ui, &process.pid, debug);
        }
    }
}
