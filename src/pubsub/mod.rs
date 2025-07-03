pub mod connection;
pub mod dispatch;
pub mod notification;
pub mod subscription;

#[derive(Debug, Clone, Copy)]
pub enum PubSubUpstreamKind {
    Chain,
    Ephem,
}
