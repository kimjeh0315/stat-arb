//! Shadow runner only. It deliberately contains no private trading client.
#![allow(dead_code)]
#[path="../model.rs"] mod model;
#[path="../book.rs"] mod book;
#[path="../exchanges.rs"] mod exchanges;
#[path="../bbo.rs"] mod bbo;
#[path="../schema.rs"] mod schema;
#[path="../trading/mod.rs"] mod trading;
#[tokio::main]
async fn main(){
 let _=rustls::crypto::ring::default_provider().install_default();
 tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
 if let Err(e)=trading::run().await{eprintln!("Shadow trader stopped: {e:#}");std::process::exit(1);}
}
