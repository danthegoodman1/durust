//! Replay-first durable workflow runtime primitives.

mod backend;
mod error;
mod history;
mod ids;
mod memory;
mod payload;
mod registry;
mod runtime;
mod worker;

pub use backend::*;
pub use durust_macros::{activity, workflow};
pub use error::{Error, Result};
pub use history::*;
pub use ids::*;
pub use memory::MemoryBackend;
pub use payload::*;
pub use registry::*;
pub use runtime::*;
pub use worker::*;

#[macro_export]
macro_rules! activity {
    ($activity:ident($input:expr)) => {
        $crate::activity_call::<$activity>($input)
    };
}
