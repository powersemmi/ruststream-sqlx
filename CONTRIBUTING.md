# Contributing to ruststream-sqlx

`ruststream-sqlx` is the SQL database crate of RustStream. The framework's core crate lives in
[`powersemmi/ruststream`](https://github.com/powersemmi/ruststream), and each broker crate in a
repository of its own. This page covers the environment, the workspace, and the checks a change
passes before review.

## Repositories

This crate builds on its own. To work on it beside the core, clone them side by side in one
directory:

```text
RustStream/
  ruststream/
  ruststream-sqlx/
  ...           the broker crates, when a change reaches them too
```

```bash
mkdir RustStream && cd RustStream
git clone https://github.com/powersemmi/ruststream.git
git clone https://github.com/powersemmi/ruststream-sqlx.git
```

## Workspace

The workspace publishes three crates, released together under one version:

- `crates/ruststream-sqlx`: the crate a service depends on;
- `crates/ruststream-sqlx-macros`: its procedural macros;
- `crates/ruststream-sqlx-dialect`: the SQL text generation shared by the macros and the runtime.

## Environment

- **Rust** through rustup. `rust-toolchain.toml` selects stable with rustfmt and clippy. The
  minimum supported version is 1.95, the `rust-version` in `Cargo.toml`:
  `rustup toolchain install 1.95` builds against it with
  `cargo +1.95 check --workspace --all-features`.
- **just**, which runs every recipe below.
- Per task:

| Task | Tool | Install |
| --- | --- | --- |
| `just deny` | cargo-deny | `cargo install cargo-deny --locked` |
| `just typo`, `just zizmor` | uv | the uv documentation |
| the documentation site | Python 3.12 | `pip install -r docs/requirements.txt`, then `properdocs serve` |

## Checking a change

```bash
just check          # rustfmt, clippy, cargo check with all features and with none, rustdoc
just test           # the test suite with all features, with none, and with the inbox alone
just ci             # check and test, plus codespell, cargo deny and zizmor
```

## Pull requests

- One logical change per pull request.
- CI runs on a pull request that is ready for review; a draft starts it when it is marked ready.
- A pull request merges as one squashed commit, after the `CI result` check and one approving
  review. Commits are signed.
- Documentation changes with the code. An item's rustdoc says what it is and does, and the crate's
  module overviews are its guides. The site holds the entry pages in English, Russian and Chinese,
  and an edit reaches all three.
