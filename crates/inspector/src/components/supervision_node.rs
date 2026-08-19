use super::debug_card::render_debug_card;
use super::status_badge::render_actor_status_badge;
use crate::theme::Theme;
use crate::utils::format_duration;
use egui::{CornerRadius, Frame, Margin, RichText, Stroke, Ui, collapsing_header::CollapsingState};
use zestors::supervision::SupervisionTree;

pub struct SupervisionNodeWidget<'a> {
    node: &'a SupervisionTree,
    default_open: bool,
}

impl<'a> SupervisionNodeWidget<'a> {
    pub fn new(node: &'a SupervisionTree, default_open: bool) -> Self {
        Self { node, default_open }
    }

    pub fn show(self, ui: &mut Ui) {
        let node_id = ui.make_persistent_id(&self.node.description.pid);

        Frame::group(ui.style())
            .fill(Theme::CARD_BG)
            .stroke(Stroke::new(1.0, Theme::BORDER_COLOR))
            .corner_radius(CornerRadius::same(6))
            .inner_margin(Margin::same(8))
            .show(ui, |ui| {
                CollapsingState::load_with_default_open(ui.ctx(), node_id, self.default_open)
                    .show_header(ui, |ui| self.render_header(ui))
                    .body(|ui| self.render_body(ui));
            });
    }

    fn render_header(&self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("PID:").small().color(Theme::LABEL_MUTED));
            ui.label(
                RichText::new(&self.node.description.pid)
                    .strong()
                    .color(Theme::PID_BLUE),
            );
            if let Some(status) = &self.node.status {
                render_actor_status_badge(ui, status);
            }
        });
    }

    fn render_body(&self, ui: &mut Ui) {
        ui.add_space(4.0);

        // Core Metadata Grid
        egui::Grid::new(ui.make_persistent_id(&format!("{}_grid", self.node.description.pid)))
            .num_columns(2)
            .spacing([12.0, 6.0])
            .show(ui, |ui| {
                ui.label(RichText::new("Restart Mode:").color(Theme::LABEL_MUTED));
                ui.label(
                    RichText::new(format!("{:?}", self.node.description.cfg.restart_mode))
                        .color(Theme::VALUE_PURPLE),
                );
                ui.end_row();

                ui.label(RichText::new("Abort Timeout:").color(Theme::LABEL_MUTED));
                ui.label(
                    RichText::new(format_duration(&self.node.description.cfg.abort_timeout))
                        .color(egui::Color32::WHITE),
                );
                ui.end_row();
            });

        // Debug Section
        if let Some(debug) = &self.node.debug_state {
            ui.add_space(6.0);
            render_debug_card(ui, &self.node.description.pid, debug);
        }

        // Child Recursion
        if !self.node.children.is_empty() {
            ui.add_space(8.0);
            ui.vertical(|ui| {
                for child in &self.node.children {
                    SupervisionNodeWidget::new(child, false).show(ui);
                    ui.add_space(4.0);
                }
            });
        }
    }
}
