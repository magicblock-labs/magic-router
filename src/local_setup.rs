use crate::config::RouterConfig;
use std::process::Command;
use tracing::{info, warn};

/// Check if the router is configured to use localhost endpoints
fn is_local_setup(config: &RouterConfig) -> bool {
    config.base_chain_urls.iter().any(|url| {
        let host = url.host_str().unwrap_or("");
        let scheme = url.scheme();
        // Detect localhost or 127.0.0.1, and http (non-https)
        (host.contains("localhost") || host.contains("127.0.0.1")) || scheme == "http"
    })
}

/// Auto-register validator if using local endpoints
pub async fn auto_register_validator_if_local(config: &RouterConfig) -> Result<(), Box<dyn std::error::Error>> {
    if !is_local_setup(config) {
        info!("Remote endpoints detected, skipping local validator setup");
        return Ok(());
    }

    info!("Local endpoints detected, attempting to auto-register validator...");

    // Try to run the register-validator binary
    let output = Command::new("cargo")
        .args(&[
            "run",
            "-p",
            "magic-router-setup",
            "--release",
            "--",
            "--rpc-url",
            config.base_chain_urls.first()
                .map(|u| u.as_str())
                .unwrap_or("http://localhost:8899"),
        ])
        .output();

    match output {
        Ok(output) => {
            if output.status.success() {
                info!("✓ Local validator registered successfully");
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                warn!(
                    "Local validator registration failed: {}",
                    stderr
                );
                // Don't fail the router startup if registration fails
                Ok(())
            }
        }
        Err(e) => {
            warn!(
                "Failed to run local validator setup: {}. Make sure the binary is built.",
                e
            );
            // Don't fail the router startup
            Ok(())
        }
    }
}
