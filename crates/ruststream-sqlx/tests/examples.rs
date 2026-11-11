//! The tests of the examples, one module per example, each gated on the example's
//! `required-features`. The manifest declares the examples.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]

ruststream_example_suite::examples!();
