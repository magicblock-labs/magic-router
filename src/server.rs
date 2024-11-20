//! server module for working with client connections

use futures::{stream::FuturesUnordered, StreamExt};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use tokio::{io::AsyncWriteExt, net::TcpListener, task::JoinHandle};

use crate::{
    config::ServerConf,
    request::handler::{process, Accessors, RequestHandler},
};

/// Incoming connections server
pub struct Server {
    /// TCP socket that server listens on
    listener: TcpListener,
    /// list of futures representing outstanding HTTP connections
    httpconnections: FuturesUnordered<JoinHandle<()>>,
    /// list of futures for websocket connections from websocket pool used by cache
    wsconnections: Vec<JoinHandle<()>>,
    /// shared resource container
    accessors: Accessors,
    /// flag indicating that SIGTERM has been received
    stopping: bool,
}

impl Server {
    /// Initialize server by binding to specified socket address
    pub async fn new(
        config: ServerConf,
        accessors: Accessors,
        wsconnections: Vec<JoinHandle<()>>,
    ) -> super::Result<Self> {
        let listener = TcpListener::bind(config.bind).await?;
        let httpconnections = Default::default();
        Ok(Self {
            listener,
            httpconnections,
            wsconnections,
            accessors,
            stopping: false,
        })
    }

    /// Run the server and serve incoming connections until SIGTERM is received
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                // hanlde new incoming tcp connections
                biased; Ok((mut stream, _addr)) = self.listener.accept() => {
                    let builder = auto::Builder::new(TokioExecutor::new());
                    if !self.stopping {
                        // we are in normal operation mode
                        let io = TokioIo::new(stream);
                        // process requests comming on that connection in a regular fashion
                        let handler = RequestHandler::build(self.accessors.clone(), process);
                        // spawn connection on a separate task, so that the tokio
                        // runtime itself we be driving the connection future
                        let conn = tokio::spawn(async move {
                            let conn = builder.serve_connection(io, handler);
                            tokio::pin!(conn);
                            // we have to loop with select! to support graceful shutdown
                            loop {
                                tokio::select! {
                                    // when future is ready, it means that connection is closed and
                                    // we can exit the loop
                                    _ = &mut conn => { break; }
                                    // if SIGTERM is received, initiate graceful shutdown for the
                                    // connection, but keep driving the conn future so that outstanding
                                    // requests can complete in a normal manner
                                    _ = crate::SHUTDOWN.notified() => {
                                        conn.as_mut().graceful_shutdown();
                                    }
                                }
                            }

                        });
                        // keep connection join handle for graceful shutdown of server
                        self.httpconnections.push(conn);
                    } else {
                        // if SIGTERM has already been received, just reject new TCP connections
                        let _ = stream.shutdown().await;
                    };
                }
                _ = crate::SHUTDOWN.notified() => {
                    self.stopping = true;
                    tracing::info!("server shutdown has been received");
                    if !self.httpconnections.is_empty() {
                        // don't break out of loop if we still have some active connections
                        continue;
                    }
                    break;
                }
                r = self.httpconnections.next() => {
                    // this branch just drives the FuturesUnordered so that
                    // only handles for active connections are kept around
                    let Some(r) = r else { continue };
                    let _ = r.inspect_err(|error| tracing::warn!(%error, "connection handler panicked"));
                    // terminate if no outstanding connections left and shutdown has been received
                    if self.httpconnections.is_empty() && self.stopping {
                        break;
                    }
                }
            }
        }
        tracing::warn!("shutting down server, no tasks left");
        // make sure that websocket connections for accounts' cache also terminate gracefully
        for connection in self.wsconnections.into_iter() {
            let _ = connection
                .await
                .inspect_err(|error| tracing::warn!(%error, "websocket connection task panicked"));
        }
    }
}
