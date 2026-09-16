//! Replay-first durable workflow runtime primitives.
//!
//! The crate root is the **workflow-authoring** surface: the `workflow` and
//! `activity` macros, the durable APIs they call, `Worker`, `Client`, and the
//! bundled backends. Everything a *provider* implements lives under
//! [`provider`], and the deterministic simulation harness lives under
//! [`testing`] behind the `testing` feature, so neither crowds the namespace a
//! workflow author works in.

mod backend;
mod error;
mod history;
mod ids;
mod manifest;
/// The map fanout engine, exposed so `tests/map_transitions.rs` can replay
/// the shared transition table; not part of the supported API.
#[doc(hidden)]
pub mod map_engine;
mod memory;
mod options;
mod payload;
mod payload_backend;
#[cfg(feature = "postgres")]
mod postgres;
mod provider_util;
mod registry;
mod runtime;
#[cfg(feature = "testing")]
mod sim;
#[cfg(feature = "sqlite")]
mod sqlite;
mod worker;

/// The surface a durability provider implements: the [`DurableBackend`] trait
/// and every request and outcome type it exchanges with the runtime, plus the
/// history events and payload plumbing a provider stores.
///
/// Workflow code needs none of this. It is a module rather than a flattened
/// re-export so that adding a provider type cannot silently widen the API a
/// workflow author sees.
pub mod provider {
    pub use crate::backend::*;
    pub use crate::history::*;
    pub use crate::payload_backend::*;
}

/// The deterministic simulation harness: a seeded scheduler, a virtual clock,
/// and a fault-injecting backend wrapper.
///
/// Behind the `testing` feature because it is test scaffolding, not something
/// a production binary should link.
#[cfg(feature = "testing")]
pub mod testing {
    pub use crate::sim::*;
}

// Crate-internal flattening: the provider modules and the runtime reach these
// as `crate::Foo`. Re-exported `pub(crate)` so that stays true without the
// public root growing a type every time a provider gains one.
#[allow(unused_imports)]
pub(crate) use backend::*;
#[allow(unused_imports)]
pub(crate) use history::*;
#[allow(unused_imports)]
pub(crate) use payload_backend::*;

pub use backend::DurableBackend;
pub use durust_macros::{activity, call_activity, child, join, query, select, workflow};
pub use error::{DurableFailure, Error, Result};
pub use ids::*;
pub use inventory;
pub use manifest::*;
pub use memory::MemoryBackend;
pub use options::*;
pub use payload::*;
#[cfg(feature = "postgres")]
pub use postgres::{PostgresBackend, PostgresBackendConfig};
pub use registry::*;
pub use runtime::*;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteBackend;
pub use worker::*;
