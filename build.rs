//! Generates the tonic messages, service and client of `serveval.proto` for the tonic pair.

fn main() -> std::io::Result<()> {
    #[cfg(feature = "tonic")]
    tonic_prost_build::configure()
        .bytes(".")
        .compile_protos(&["serveval.proto"], &["."])?;
    Ok(())
}
