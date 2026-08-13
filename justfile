[private]
default:
    @just -l -u --list-submodules

supervise-example:
    @cargo run --example supervision

generate-openapi:
    @cargo run --package zestors-gen-oapi