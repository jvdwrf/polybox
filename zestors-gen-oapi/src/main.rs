fn main() {
    let api = zestors::node::ApiConfig::generate_openapi();
    let json = serde_json::to_string_pretty(&api).unwrap();
    std::fs::write("openapi.json", json).unwrap();
}
