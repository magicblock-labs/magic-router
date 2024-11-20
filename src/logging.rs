//! module for working with logs

use std::{io::stdout, str::FromStr};

use tracing_appender::{non_blocking, non_blocking::WorkerGuard, rolling::RollingFileAppender};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::config::{LogFormat, LoggingConf, LoggingMode};

/// Initialize tracing based logger
pub fn init(config: LoggingConf) -> WorkerGuard {
    let filter = config
        .level
        .as_ref()
        .and_then(|l| EnvFilter::from_str(l).ok())
        .unwrap_or_else(EnvFilter::from_default_env);

    let (output, guard) = match config.mode {
        LoggingMode::Stdout => non_blocking(stdout()),
        LoggingMode::File { path } => non_blocking(
            std::fs::File::create(path)
                .inspect_err(|e| eprintln!("failed to open log file: {e}"))
                .expect("log file should exist"),
        ),
        LoggingMode::Rotating { dir, rotation } => {
            let file_appender = RollingFileAppender::new(rotation, dir, env!("CARGO_PKG_NAME"));
            non_blocking(file_appender)
        }
    };
    let subscriber = FmtSubscriber::builder()
        .with_writer(output)
        .with_env_filter(filter);
    match config.format {
        LogFormat::Plain => tracing::subscriber::set_global_default(subscriber.compact().finish()),
        LogFormat::Json => tracing::subscriber::set_global_default(subscriber.json().finish()),
    }
    .inspect_err(|e| eprintln!("failed to init global logging: {e}"))
    .expect("log init should not fail");
    guard
}
