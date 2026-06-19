fn main() -> Result<(), Box<dyn std::error::Error>> {
    // protox is a pure-Rust protoc implementation — no external binary required.
    let file_descriptors = protox::compile(
        [
            "../../proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
            "../../proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
            "../../proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
        ],
        ["../../proto"],
    )?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(file_descriptors)?;

    // Re-run if any proto file changes.
    println!("cargo:rerun-if-changed=../../proto");

    Ok(())
}
