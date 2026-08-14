[private]
default:
    @just -l -u --list-submodules

supervise-example:
    @cargo run --example supervision

generate-openapi:
    @cargo run --package zestors-gen-oapi

generate-api-client: generate-openapi
    docker run --rm \
        -v "$PWD:/local" \
        openapitools/openapi-generator-cli generate \
        -i /local/openapi.json \
        -g rust \
        -o /local/crates/api-client \
        --library reqwest \
        --additional-properties=packageName=zestors-api-client \
        --additional-properties=packageVersion=0.1.0

mod inspector "crates/inspector"