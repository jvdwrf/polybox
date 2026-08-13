[private]
default:
    @just -l -u --list-submodules

supervise-example:
    @cargo run --example supervision