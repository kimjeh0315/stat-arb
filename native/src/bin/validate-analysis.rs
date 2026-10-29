#[path = "../validation/mod.rs"]
mod validation;

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if let Err(error) = validation::run().await {
        eprintln!("Validation failed: {error:#}");
        std::process::exit(1);
    }
}
