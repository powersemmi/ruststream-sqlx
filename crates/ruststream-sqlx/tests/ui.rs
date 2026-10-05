//! Compile-fail snapshots of the diagnostics `#[derive(Inbox)]` owns.
//!
//! Each `tests/ui/*.rs` case feeds the derive a struct that cannot drive a queue and pins the
//! compile error against a `.stderr` snapshot, so a reworded or dropped message, or a span that
//! moved off the offending field, fails the build.
//!
//! The snapshots record one toolchain's exact wording, so they run on the stable toolchain this
//! repository selects, and only when `RUN_UI_TESTS=1`. To refresh them after an intentional
//! message change, run on stable and read every changed snapshot before committing it:
//!
//! ```text
//! TRYBUILD=overwrite RUN_UI_TESTS=1 cargo test -p ruststream-sqlx --all-features --test ui
//! ```

#![cfg(feature = "inbox")]

use std::env;

/// Whether this run skips the snapshots, and whether skipping them is allowed.
///
/// `RUN_UI_TESTS=1` opts in. `REQUIRE_UI_TESTS=1` says a skip is not acceptable in this run: the
/// gates set it wherever they set the opt-in, so a dropped or misspelled opt-in fails the run
/// instead of passing with nothing checked.
///
/// # Panics
///
/// Panics when a run that requires the snapshots is not set up to run them.
fn skip_snapshots() -> bool {
    let opted_in = env::var("RUN_UI_TESTS").as_deref() == Ok("1");
    let required = env::var("REQUIRE_UI_TESTS").as_deref() == Ok("1");
    assert!(
        opted_in || !required,
        "REQUIRE_UI_TESTS=1 but RUN_UI_TESTS is not 1: this run would have skipped the UI \
         snapshots and reported success"
    );
    !opted_in
}

#[test]
fn ui() {
    if skip_snapshots() {
        eprintln!("skipping trybuild UI tests; set RUN_UI_TESTS=1 (stable toolchain) to run them");
        return;
    }
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
