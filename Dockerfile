# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.87-slim-bookworm AS builder

WORKDIR /build

# Install system dependencies needed by tonic-build and parquet.
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies by copying manifests first.
COPY Cargo.toml Cargo.lock ./
COPY vendor/ vendor/
COPY crates/infralens-common/Cargo.toml   crates/infralens-common/
COPY crates/infralens-proto/Cargo.toml    crates/infralens-proto/
COPY crates/infralens-storage/Cargo.toml  crates/infralens-storage/
COPY crates/infralens-ingest/Cargo.toml   crates/infralens-ingest/
COPY crates/infralens-cluster/Cargo.toml  crates/infralens-cluster/
COPY crates/infralens-rpc/Cargo.toml      crates/infralens-rpc/
COPY crates/infralens-query/Cargo.toml    crates/infralens-query/
COPY crates/infralens-server/Cargo.toml   crates/infralens-server/

# Create stub src files so cargo can resolve the workspace without full source.
RUN for d in common proto storage ingest cluster rpc query; do \
      mkdir -p crates/infralens-$d/src && echo "// stub" > crates/infralens-$d/src/lib.rs; \
    done && \
    mkdir -p crates/infralens-server/src && echo "fn main(){}" > crates/infralens-server/src/main.rs

RUN cargo build --release 2>/dev/null || true

# Purge stale stub artifacts so the real build fully recompiles workspace members.
# External crate artifacts (everything NOT infralens_*) are preserved for caching.
RUN find target -name "*.rlib"  -path "*infralens*" -delete 2>/dev/null || true; \
    find target -name "*.rmeta" -path "*infralens*" -delete 2>/dev/null || true; \
    find target/.fingerprint -maxdepth 1 -name "infralens*" -type d \
         -exec rm -rf {} + 2>/dev/null || true

# Now copy real source and proto files.
COPY proto/ proto/
COPY crates/ crates/

RUN cargo build --release --bin infralens-server

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/infralens-server /app/infralens-server

EXPOSE 4317 4318 9090

ENTRYPOINT ["/app/infralens-server"]
