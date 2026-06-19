fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fds = protox::compile(
        ["../../proto/infralens/internal/v1/internal.proto"],
        ["../../proto"],
    )?;
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;
    println!("cargo:rerun-if-changed=../../proto/infralens");
    Ok(())
}
