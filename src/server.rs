use std::{net::SocketAddr, sync::Arc};

use serde::Deserialize;
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};

pub struct Server {
    listener: TcpListener,
    shutdown: Arc<Notify>,
}

#[derive(Deserialize)]
pub struct ServerConf {
    bind: SocketAddr,
}

impl Server {
    pub async fn new(config: ServerConf, shutdown: Arc<Notify>) -> super::Result<Self> {
        let listener = TcpListener::bind(config.bind).await?;
        Ok(Self { listener, shutdown })
    }

    pub async fn start(self) {
        loop {
            tokio::select! {
                Ok((stream, addr)) = self.listener.accept() => {
                }
                _ = self.shutdown.notified() => {}
                else => {
                    break;
                }
            }
        }
    }
}
