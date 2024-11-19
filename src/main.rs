mod connection;
mod error;
mod request;
mod server;

type Result<T> = std::result::Result<T, error::Error>;

fn main() {
    println!("Hello, world!");
}
