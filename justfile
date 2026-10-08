set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

# The scenarios `just bench-code` counts, one benchmark file each. Every one is gated: each run
# holds them to their allocation limits, and a run against a baseline to the instruction limit.
code_benches := "--bench consume --bench reply --bench batch --bench lease --bench advisory --bench by_name --bench row_mode --bench publish --bench outbox_untracked --bench outbox_publish --bench outbox_delivery --bench outbox"

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the crate's `testing` feature is a compile error in it. Its
    # own legs follow.
    cargo clippy --workspace --exclude ruststream-sqlx-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-sqlx-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-sqlx-bench --all-targets --all-features
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
    # The outbox without the inbox: its doc examples gate themselves on the outbox alone.
    cargo test -p ruststream-sqlx --doc --no-default-features --features outbox,postgres

brokers-up:
    docker compose -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -f docker-compose.test.yml down -v

# Runs the suites against the compose stand's Postgres, MySQL and MariaDB. RUSTSTREAM_REQUIRE_LIVE
# turns a skipped live test into a failure, so a stand the suites never reached is reported instead
# of passing. RUSTSTREAM_SQLX_OUTBOX turns the outbox on in this test build; `just test` runs with
# it off, the way a service's own tests run.
test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    POSTGRES_TEST_URL=postgres://ruststream:ruststream@127.0.0.1:55432/ruststream \
    MYSQL_TEST_URL=mysql://root:ruststream@127.0.0.1:53306 \
    MARIADB_TEST_URL=mysql://root:ruststream@127.0.0.1:53307 \
    RUSTSTREAM_REQUIRE_LIVE=1 \
    RUSTSTREAM_SQLX_OUTBOX=on \
        cargo test -p ruststream-sqlx --all-features --no-fail-fast

# The three published crates packaged and verified together, as the release publishes them:
# the dialect, then the macros, then the crate. Nothing is uploaded.
package:
    cargo publish --locked --dry-run --all-features -p ruststream-sqlx-dialect -p ruststream-sqlx-macros -p ruststream-sqlx

# Regenerates the offline query data of the checked fixtures (`tests/checked/*`) against the
# stand (`just brokers-up`), each fixture in a database of its own rebuilt from its migrations, and
# checks it. Needs sqlx-cli 0.9:
# cargo install sqlx-cli --version 0.9.0 --locked --no-default-features --features postgres,mysql,sqlite,rustls,sqlx-toml
sqlx-prepare:
    #!/usr/bin/env bash
    set -euo pipefail
    # sqlx's macros write the query data while they run. A compiler cache that returns a crate it
    # built before runs none of them, so `prepare` would write nothing and `--check` would pass
    # without checking: the recipe turns kache off.
    export KACHE_DISABLED=1
    sqlite=$(mktemp -d)
    trap 'rm -rf "$sqlite"' EXIT
    prepare() {
        (
            cd "tests/checked/$1"
            export DATABASE_URL="$2"
            sqlx database reset -y --source migrations
            cargo sqlx prepare
            cargo sqlx prepare --check
        )
    }
    prepare postgres postgres://ruststream:ruststream@127.0.0.1:55432/ruststream_checked
    prepare mysql mysql://root:ruststream@127.0.0.1:53306/ruststream_checked
    prepare sqlite "sqlite://$sqlite/checked.db"

# What this crate, and then the runtime above it, cost over the sqlx statements they run, against
# the stand the tests use: every scenario run as a raw sqlx loop, as this crate driven by hand and
# as a RustStream service, timed on a multi-threaded runtime, then the throughput grid, a filled
# table drained by n workers on Postgres, MySQL and SQLite. The outbox's scenarios compare one app
# over the stand's Redis in three variants: no outbox, the outbox written by hand, this crate's.
# On demand only - it takes about half an hour and it wants the machine to itself. The page it
# feeds is docs/benchmarks.md. `RUSTSTREAM_BENCH_SECONDS` sets the shortest wall-clock run,
# `RUSTSTREAM_BENCH_THROUGHPUT_ROWS` the rows of a throughput run, `RUSTSTREAM_BENCH_PAIRS` the
# rounds.
bench *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    export RUSTFLAGS="" \
        POSTGRES_TEST_URL=postgres://ruststream:ruststream@127.0.0.1:55432/ruststream \
        MYSQL_TEST_URL=mysql://root:ruststream@127.0.0.1:53306 \
        REDIS_URL=redis://127.0.0.1:56379
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-sqlx-bench --bench paired {{ ARGS }}
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-throughput.json" \
        cargo bench -p ruststream-sqlx-bench --bench throughput {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json
    python3 scripts/bench_results.py --throughput target/bench-throughput.json \
        docs/benchmarks/results.json

# What a message costs in this crate, the framework and sqlx's driver on the service's thread,
# counted under valgrind: instructions through callgrind and allocations through DHAT, each
# scenario a service on the production broker and the raw sqlx loop beside it, against the
# stand's Postgres; an outbox scenario is one app over `MemoryBroker` with no outbox, with the
# outbox written by hand, and with this crate's. The page it feeds is the code table of docs/benchmarks.md. RUSTFLAGS is
# cleared because valgrind aborts on the instructions a recent CPU advertises. Needs valgrind.
#
# The benchmarks hand the measurement to gungraun's runner, which has to be the release of the
# library the lock file pins. The recipe installs that release into `target/gungraun-runner` on
# the first run and after the library moves, and puts it first on PATH, where the benchmarks look
# the runner up. A `GUNGRAUN_RUNNER` in the environment would win over PATH when the benchmarks
# build, so the recipe clears it.
#
# A leading number is the deliveries per measured run: the default of 1000 is what the published
# document is measured at, a larger count buys a steadier number for a longer run
# (`just bench-code 5000`). The benches read it at build time, so a new count rebuilds them. The
# other arguments reach the benchmark runner: `just bench-code --save-baseline=main` records a
# baseline, `just bench-code --baseline=main` measures against it. Totals over another count are
# not comparable, so each count keeps its runs and baselines in a directory of its own,
# `target/gungraun/<count>`.
#
# A run against a baseline, named with `--baseline` or in `GUNGRAUN_BASELINE`, fails on two
# percent more instructions than the baseline in a scenario. The limit is relative, so it applies
# only there: a plain run would be held to whichever run came before it, on whatever tree that
# was. The allocation limits are absolute, and every run is held to them.
#
# A benchmark that breaches a limit fails the run, and the run still goes to the end: the table
# prints, every breach under it with the value it was compared against beside the new one, and
# the recipe fails after that. A build error stops it before anything runs.
[positional-arguments]
bench-code *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    messages=1000
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
        messages="$1"
        shift
    fi
    version="$(cargo pkgid gungraun)"
    version="${version##*@}"
    runner="$PWD/target/gungraun-runner"
    installed="$("$runner/bin/gungraun-runner" --version 2> /dev/null || true)"
    if [ "$installed" != "gungraun-runner $version" ]; then
        cargo install --locked --root "$runner" gungraun-runner --version "=$version"
    fi
    unset GUNGRAUN_RUNNER
    export PATH="$runner/bin:$PATH" RUSTFLAGS="" \
        POSTGRES_TEST_URL=postgres://ruststream:ruststream@127.0.0.1:55432/ruststream \
        RUSTSTREAM_BENCH_MESSAGES="$messages" GUNGRAUN_HOME="$PWD/target/gungraun/$messages"
    # A baseline named on the command line or in the environment brings the instruction limit.
    baseline="${GUNGRAUN_BASELINE:-}"
    for arg in "$@"; do
        case "$arg" in --baseline | --baseline=*) baseline="$arg" ;; esac
    done
    limits=()
    if [ -n "$baseline" ]; then
        limits=(--callgrind-limits='ir=2.0%')
    fi
    mkdir -p target
    cargo bench -p ruststream-sqlx-bench {{ code_benches }} --no-run
    status=0
    cargo bench -p ruststream-sqlx-bench {{ code_benches }} --no-fail-fast \
        -- --output-format=json "${limits[@]}" "$@" > target/bench-code.json || status=$?
    python3 scripts/bench_results.py --code --messages "$messages" target/bench-code.json \
        docs/benchmarks/results.json
    exit "$status"

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
