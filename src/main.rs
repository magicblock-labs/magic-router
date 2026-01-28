use std::{env, fs::read_to_string};

use router::RouterResult;

use router::config::RouterConfig;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> RouterResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config_path = env::args()
        .nth(1)
        .expect("usage: magic-router <path-to-config.toml>");
    let config = read_to_string(config_path)?;
    let config: RouterConfig =
        toml::from_str(&config).expect("failed to parse router configuration file");
    
    let handle = router::run(config).await?;
    tracing::info!("Router is ready and running!");
    tokio::signal::ctrl_c().await?;
    let _ = handle.stop();
    handle.stopped().await;
    Ok(())
}
