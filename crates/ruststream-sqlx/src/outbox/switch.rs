//! The switch of test builds: the outbox is off unless `RUSTSTREAM_SQLX_OUTBOX=on`.

#[cfg(feature = "testing")]
use std::env;
#[cfg(feature = "testing")]
use std::sync::LazyLock;

/// The environment variable that turns the outbox on in a test build.
#[cfg(feature = "testing")]
pub(crate) const SWITCH: &str = "RUSTSTREAM_SQLX_OUTBOX";

/// Whether the outbox tracks messages in this process.
///
/// A production build always tracks. A test build (the `testing` feature) tracks only with the
/// switch set, read once per process: Rust 2024 makes `std::env::set_var` unsafe and tests share
/// one process, so the switch is set outside the tests.
#[cfg(not(feature = "testing"))]
#[inline]
pub(crate) const fn enabled() -> bool {
    true
}

/// Whether the outbox tracks messages in this process; see the production build's `enabled`.
#[cfg(feature = "testing")]
pub(crate) fn enabled() -> bool {
    // Why a static: the switch is a property of the process, read once, as the issue specifies.
    static ON: LazyLock<bool> = LazyLock::new(|| reads_on(env::var(SWITCH).ok().as_deref()));
    *ON
}

/// Whether a value of the switch turns the outbox on.
#[cfg(any(feature = "testing", test))]
fn reads_on(value: Option<&str>) -> bool {
    value == Some("on")
}

#[cfg(test)]
mod tests {
    use super::reads_on;

    #[test]
    fn only_on_turns_the_outbox_on() {
        assert!(reads_on(Some("on")));
        assert!(!reads_on(None));
        assert!(!reads_on(Some("")));
        assert!(!reads_on(Some("1")));
        assert!(!reads_on(Some("ON")));
    }
}
