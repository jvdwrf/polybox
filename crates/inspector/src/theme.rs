use egui::{Color32, Context, Stroke, Visuals};

pub struct Theme;

impl Theme {
    pub const BG_DARK: Color32 = Color32::from_rgb(18, 20, 26);
    pub const CARD_BG: Color32 = Color32::from_rgb(26, 29, 38);
    pub const INNER_CARD_BG: Color32 = Color32::from_rgb(20, 22, 30);
    pub const BORDER_COLOR: Color32 = Color32::from_rgb(45, 50, 66);
    pub const PID_BLUE: Color32 = Color32::from_rgb(97, 175, 239);
    pub const LABEL_MUTED: Color32 = Color32::from_rgb(140, 148, 170);
    pub const VALUE_PURPLE: Color32 = Color32::from_rgb(198, 120, 221);
    pub const ERROR_RED: Color32 = Color32::from_rgb(224, 108, 117);

    pub fn apply(ctx: &Context) {
        let mut visuals = Visuals::dark();
        visuals.panel_fill = Self::BG_DARK;
        visuals.window_fill = Self::BG_DARK;
        visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, Self::BORDER_COLOR);
        ctx.set_visuals(visuals);
    }

    /// Dynamic Status Badge Colors (Background, Foreground Text)
    pub fn status_colors(status: &str) -> (Color32, Color32) {
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
}
