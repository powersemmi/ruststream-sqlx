set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo check --workspace --all-targets --all-features
    cargo check --workspace --no-default-features
    # Rustdoc sees what rustc cannot: broken intra-doc links and redundant targets. CI gates on
    # it too.
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
    # The compile-fail snapshots of the derives: rustc's exact wording on the stable toolchain
    # this repository selects. REQUIRE_UI_TESTS turns a skip into a failure, so a lost opt-in
    # shows here.
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test -p ruststream-sqlx --all-features --test ui

test:
    cargo test --workspace --all-features
    # Both feature edges: an all-features run hides a doc example that names a feature-gated
    # item without gating itself.
    cargo test --workspace --no-default-features
    # The inbox without the built-in dialect, as a service with a dialect of its own builds it.
    cargo test -p ruststream-sqlx --no-default-features --features inbox

brokers-up:
    docker compose -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -f docker-compose.test.yml down -v

# Runs the suites against the compose stand's Postgres. RUSTSTREAM_REQUIRE_LIVE turns a skipped
# live test into a failure, so a stand the suites never reached is reported instead of passing.
test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    POSTGRES_TEST_URL=postgres://ruststream:ruststream@127.0.0.1:55432/ruststream \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test -p ruststream-sqlx --all-features --no-fail-fast

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean
    rm -rf dist wheels

ci: check test typo security
