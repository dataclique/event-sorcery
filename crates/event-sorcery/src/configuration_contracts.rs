//! Compile-time contracts for explicit public trait configuration.
//!
//! A fully configured consumer remains valid.
//!
//! ```
//! # fn main() {}
//! # include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/public_configuration.rs"));
//! entity_configuration! {
//!     const SNAPSHOT_SIZE: usize = 10;
//!     const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;
//! }
//! job_configuration! {
//!     const TERMINAL_FAILURE_MSG: &'static str = "Job failed after retries";
//! }
//! ```
//!
//! Snapshot cadence cannot be omitted.
//!
//! ```compile_fail,E0046
//! # fn main() {}
//! # include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/public_configuration.rs"));
//! entity_configuration! {
//!     const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;
//! }
//! ```
//!
//! Event retention cannot be omitted.
//!
//! ```compile_fail,E0046
//! # fn main() {}
//! # include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/public_configuration.rs"));
//! entity_configuration! {
//!     const SNAPSHOT_SIZE: usize = 10;
//! }
//! ```
//!
//! The standalone job's terminal-failure message cannot be omitted.
//!
//! ```compile_fail,E0046
//! # fn main() {}
//! # include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/public_configuration.rs"));
//! job_configuration! {}
//! ```
