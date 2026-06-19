fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=proto/auth.proto");
    println!("cargo:rerun-if-changed=proto/kv.proto");
    println!("cargo:rerun-if-changed=proto/rpc.proto");
    println!("cargo:rerun-if-changed=proto/v3election.proto");
    println!("cargo:rerun-if-changed=proto/v3lock.proto");

    let file_descriptors = protox::compile(
        [
            "proto/auth.proto",
            "proto/kv.proto",
            "proto/rpc.proto",
            "proto/v3election.proto",
            "proto/v3lock.proto",
        ],
        ["proto"],
    )?;

    tonic_build::configure()
        .build_server(false)
        .compile_fds(file_descriptors)?;

    Ok(())
}
