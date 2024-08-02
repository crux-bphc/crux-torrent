use super::PeerAddr;
use super::PeerAlerts;
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::prelude::*;
use crate::torrent::{InfoHash, PeerId};

use super::descriptor::WorkerStateDescriptor;
use super::worker_fsm::WorkerState;

use crate::peer_protocol::codec::{self, PeerMessage};
use crate::peer_protocol::handshake::PeerHandshake;

#[derive(Debug, Clone)]
pub struct PeerConnector {
    peer_addr: PeerAddr,
}

/// interface type between PeerAddr and PeerDownloadWorker
#[derive(Debug)]
pub struct PeerDownloaderConnection {
    peer_addr: PeerAddr,
    peer_id: PeerId,
    stream: TcpStream,
}

#[derive(Debug)]
pub struct PeerDownloadWorker {
    state: WorkerState,
    descriptor: WorkerStateDescriptor,
}

impl PeerConnector {
    pub fn new(peer_addr: PeerAddr) -> Self {
        Self { peer_addr }
    }

    #[instrument(name = "handshake mode", level = "info", skip_all)]
    pub async fn handshake(
        self,
        info_hash: InfoHash,
        peer_id: PeerId,
    ) -> anyhow::Result<PeerDownloaderConnection> {
        info!("connecting to peer");
        let mut stream = TcpStream::connect(&self.peer_addr).await?;

        let handshake = PeerHandshake::new(info_hash, peer_id);
        let mut bytes = handshake.into_bytes();

        info!("sending handshake to peer");
        stream.write_all(&bytes).await?;

        info!("waiting for peer handshake");
        stream.read_exact(&mut bytes).await?;

        let handshake = PeerHandshake::from_bytes(bytes);
        info!("peer handshake received");
        debug!(peer_handshake_reply = ?handshake);

        Ok(PeerDownloaderConnection {
            stream,
            peer_id: handshake.peer_id,
            peer_addr: self.peer_addr,
        })
    }
}

impl PeerDownloadWorker {
    const COMMAND_BUFFER_SIZE: usize = 5;

    pub async fn init_from(
        PeerDownloaderConnection {
            stream,
            peer_id,
            peer_addr,
            ..
        }: PeerDownloaderConnection,
        alerts_tx: mpsc::Sender<PeerAlerts>,
    ) -> anyhow::Result<PeerDownloadWorker> {
        let mut peer_stream = codec::upgrade_stream(stream);

        let msg = match peer_stream.next().await {
            Some(msg_res) => msg_res?,
            None => {
                warn!("peer closed connection before handshake");
                anyhow::bail!("peer closed connection before handshake");
            }
        };

        type PM = PeerMessage;
        let bitfield = match msg {
            PM::Bitfield(bitfield) => bitfield,
            _ => {
                warn!("first message sent by peer was not a bitfield");
                anyhow::bail!("first message sent by peer not bitfield {:?}", msg);
            }
        };

        let (commands_tx, commands_rx) = mpsc::channel(Self::COMMAND_BUFFER_SIZE);

        info!("sending init peer alert to engine");
        alerts_tx
            .send(PeerAlerts::InitPeer {
                peer_addr,
                bitfield,
                commands_tx,
            })
            .await?;
        let descriptor =
            WorkerStateDescriptor::new(peer_stream, peer_addr, peer_id, alerts_tx, commands_rx);

        Ok(Self {
            descriptor,
            state: WorkerState::Idle,
        })
    }

    pub async fn start_peer_event_loop(&mut self) -> anyhow::Result<()> {
        loop {
            self.state.transition(&mut self.descriptor).await?;
        }
    }
}
