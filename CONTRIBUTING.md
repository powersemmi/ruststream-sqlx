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

The version is the workspace's, and each crate pins the others at it exactly. A release publishes
the dialect first, then the macros, then the crate. `just package` packages and verifies the three
the same way, without uploading anything; CI runs it on every change to the crates.

## Environment

- **Rust** through rustup. `rust-toolchain.toml` selects stable with rustfmt and clippy. The
  minimum supported version is 1.95, the `rust-version` in `Cargo.toml`:
  `rustup toolchain install 1.95` builds against it with
  `cargo +1.95 check --workspace --all-features`.
- **just**, which runs every recipe below.
- Per task:

| Task | Tool | Install |
| --- | --- | --- |
| `just test-brokers` | Docker with the compose plugin | the Docker documentation |
| `just deny` | cargo-deny | `cargo install cargo-deny --locked` |
| `just typo`, `just zizmor` | uv | the uv documentation |
| the documentation site | Python 3.12 | `pip install -r docs/requirements.txt`, then `properdocs serve` |

## Checking a change

```bash
just check          # rustfmt, clippy, cargo check with all features and with none, rustdoc,
                    # and the compile-fail snapshots of the derives
just test           # the test suite with all features, with none, and with the inbox alone
just ci             # check and test, plus codespell, cargo deny and zizmor
```

The inbox's live tests run against the three servers of `docker-compose.test.yml`: Postgres 17,
MySQL 8.0 and MariaDB 10.6. The SQLite suites run in memory, with no server, and never skip.

```bash
just test-brokers   # starts the stand, runs the crate's suite against it, stops the stand
```

A live test reads each server's address from its variable and creates a database of its own
there:

| Server | Variable | Address |
| --- | --- | --- |
| Postgres | `POSTGRES_TEST_URL` | `postgres://ruststream:ruststream@127.0.0.1:55432/ruststream` |
| MySQL | `MYSQL_TEST_URL` | `mysql://root:ruststream@127.0.0.1:53306` |
| MariaDB | `MARIADB_TEST_URL` | `mysql://root:ruststream@127.0.0.1:53307` |

Without its variable a server's test skips, so `just test` passes on a machine with no Docker.
`RUSTSTREAM_REQUIRE_LIVE` turns that skip into a failure; `just test-brokers` and CI set it, so a
suite that never reached the stand cannot pass. `just brokers-up` and `just brokers-down` start
and stop the stand alone, for running one live test by hand:

```bash
just brokers-up
POSTGRES_TEST_URL=postgres://ruststream:ruststream@127.0.0.1:55432/ruststream \
MYSQL_TEST_URL=mysql://root:ruststream@127.0.0.1:53306 \
MARIADB_TEST_URL=mysql://root:ruststream@127.0.0.1:53307 \
    cargo test -p ruststream-sqlx --all-features --test conformance
just brokers-down
```

The compile-fail snapshots record the stable toolchain's wording. After an intentional change to
a message, refresh them and read every changed snapshot before committing it:

```bash
TRYBUILD=overwrite RUN_UI_TESTS=1 cargo test -p ruststream-sqlx --all-features --test ui
```

## Pull requests

- One logical change per pull request.
- CI runs on a pull request that is ready for review; a draft starts it when it is marked ready.
- A pull request merges as one squashed commit, after the `CI result` check and one approving
  review. Commits are signed.
- Documentation changes with the code. An item's rustdoc says what it is and does, and the crate's
  module overviews are its guides. The site holds the entry pages in English, Russian and Chinese,
  and an edit reaches all three.
