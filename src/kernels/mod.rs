//! Pure decision kernels: functions from inputs to decisions, shared by
//! production code and the model checkers (`tests/model*.rs`).
//!
//! Rule: nothing in this module imports from the rest of the crate except
//! plain data types, and nothing here touches tokio, async-nats, futures or
//! `std::io`. No I/O, no async, no clock.

// Consumers: `applied::Fold`, `repair::fold_in`, `tests/model_applied.rs`,
// `tests/model_resync_order.rs` (re-exported as `cursor_kernel`).
pub mod applied;
pub mod protocol;
