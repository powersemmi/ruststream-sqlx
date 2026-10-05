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

test:
    cargo test --workspace --all-features
    # Both feature edges: an all-features run hides a doc example that names a feature-gated
    # item without gating itself.
    cargo test --workspace --no-default-features

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
