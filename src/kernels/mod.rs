//! Pure decision kernels: functions from inputs to decisions, shared by
//! production code and the model checkers (`tests/model*.rs`).
//!
//! Rule: nothing in this module imports from the rest of the crate except
//! plain data types, and nothing here touches tokio, async-nats, futures or
//! `std::io`. No I/O, no async, no clock.

pub mod protocol;
