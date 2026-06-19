//! Generated OTLP protobuf types and tonic gRPC service stubs.
//!
//! The code in this module is produced by `tonic_build` from the proto files
//! in `../../proto/` during the Cargo build step (see `build.rs`).

// ── Common types ──────────────────────────────────────────────────────────────
pub mod common {
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.common.v1");
    }
}

// ── Resource ──────────────────────────────────────────────────────────────────
pub mod resource {
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.resource.v1");
    }
}

// ── Signals ───────────────────────────────────────────────────────────────────
pub mod logs {
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.logs.v1");
    }
}

pub mod metrics {
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.metrics.v1");
    }
}

pub mod trace {
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.trace.v1");
    }
}

// ── Collector service stubs ───────────────────────────────────────────────────
pub mod collector {
    pub mod logs {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
        }
    }
    pub mod metrics {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.collector.metrics.v1");
        }
    }
    pub mod trace {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.collector.trace.v1");
        }
    }
}
