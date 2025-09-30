pub mod connection;
pub mod dispatch;
pub mod laser;
pub mod notification;
pub mod subscription;

#[derive(Debug, Clone, Copy)]
pub enum PubSubUpstreamKind {
    Chain,
    Ephem,
}
