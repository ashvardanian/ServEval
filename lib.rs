//! Flags, payloads, reports, TLS and latency statistics shared by every Rust pair.

pub mod arguments;
pub mod payloads;
/// The messages, service and client of `serveval.proto`, for the tonic pair.
#[cfg(feature = "tonic")]
pub mod proto {
    tonic::include_proto!("serveval");
}
pub mod results;
pub mod security;
pub mod stats;
