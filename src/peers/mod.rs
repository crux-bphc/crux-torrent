use std::net::SocketAddrV4;

pub mod download_worker;

mod progress;

pub type PeerAddr = SocketAddrV4;
pub type PieceIndex = usize;
pub type PieceLength = u32;
type BlockLength = u32;
type BlockOffset = u32;
