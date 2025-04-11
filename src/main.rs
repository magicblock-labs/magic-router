use error::RouterError;

type RouterResult<T> = Result<T, RouterError>;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
}

mod accounts;
mod cache;
mod config;
mod error;
mod pubsub;
mod rpc;
mod server;
mod types;
