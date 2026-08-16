use crate::theme::Theme;
use egui::{CornerRadius, Frame, Margin, RichText, Ui};

pub fn render_status_badge(ui: &mut Ui, status: &str) {
    let (badge_bg, badge_fg) = Theme::status_colors(status);

    Frame::canvas(ui.style())
        .fill(badge_bg)
        .corner_radius(CornerRadius::same(4))
        .inner_margin(Margin::symmetric(6, 2))
        .show(ui, |ui| {
            ui.label(RichText::new(status).small().strong().color(badge_fg));
        });
}
