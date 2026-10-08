//! Compile-fail snapshots of what a checked struct refuses where the struct alone cannot tell: a
//! message that reads the rows of a checked headers struct with the crate's own fetch.
//!
//! The snapshots record one toolchain's exact wording, so they run on the stable toolchain this
//! repository selects, and only when `RUN_UI_TESTS=1`; `REQUIRE_UI_TESTS=1` turns a skip into a
//! failure. To refresh them, run on stable and read every changed snapshot before committing it:
//!
//! ```text
//! TRYBUILD=overwrite RUN_UI_TESTS=1 cargo test -p ruststream-sqlx-checked-postgres --test ui
//! ```

#![cfg(feature = "checked")]

use std::env;

#[test]
fn ui() {
    let opted_in = env::var("RUN_UI_TESTS").as_deref() == Ok("1");
    let required = env::var("REQUIRE_UI_TESTS").as_deref() == Ok("1");
    assert!(
        opted_in || !required,
        "REQUIRE_UI_TESTS=1 but RUN_UI_TESTS is not 1: this run would have skipped the UI \
         snapshots and reported success"
    );
    if !opted_in {
        eprintln!("skipping trybuild UI tests; set RUN_UI_TESTS=1 (stable toolchain) to run them");
        return;
    }
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
