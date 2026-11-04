#[path = "../research/mod.rs"]
mod research;

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if let Err(error) = research::run().await {
        eprintln!("Research failed: {error:#}");
        std::process::exit(1);
    }
}
