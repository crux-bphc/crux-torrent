use crate::peer_protocol::codec::PeerFrames;
use tokio::sync::mpsc;

use super::{PeerAddr, PeerAlerts, PeerCommands};
use std::collections::VecDeque;

use super::PieceRequestInfo;
use crate::PeerId;
use tokio::net::TcpStream;

#[derive(Debug)]
/// data struct that owns all the types which describe the state of the worker independent of the
/// download state.
pub(super) struct WorkerStateDescriptor {
    pub peer_addr: PeerAddr,
    pub peer_id: PeerId,
    pub peer_stream: PeerFrames<TcpStream>,
    pub commands_rx: mpsc::Receiver<PeerCommands>,
    pub alerts_tx: mpsc::Sender<PeerAlerts>,
    pub download_queue: VecDeque<PieceRequestInfo>,
    pub peer_is_choked: bool,
    pub we_are_interested: bool,
}

impl WorkerStateDescriptor {
    pub fn new(
        peer_stream: PeerFrames<TcpStream>,
        peer_addr: PeerAddr,
        peer_id: PeerId,
        alerts_tx: mpsc::Sender<PeerAlerts>,
        commands_rx: mpsc::Receiver<PeerCommands>,
    ) -> Self {
        Self {
            peer_stream,
            peer_addr,
            peer_id,
            alerts_tx,
            commands_rx,
            peer_is_choked: true,
            we_are_interested: false,
            download_queue: VecDeque::new(),
        }
    }
}
