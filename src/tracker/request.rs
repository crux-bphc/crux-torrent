use rand::distributions::{Alphanumeric, DistString};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PeerId([u8; Self::PEER_ID_SIZE]);

impl PeerId {
    pub const PEER_ID_SIZE: usize = 20;
    pub const PEER_ID_VENDOR_PREFIX: &'static [u8; 8] = b"-CX0000-";
    const PREFIX_LEN: usize = Self::PEER_ID_VENDOR_PREFIX.len();
    const SUFFIX_LEN: usize = Self::PEER_ID_SIZE - Self::PREFIX_LEN;

    pub fn new(suffix: &[u8; Self::SUFFIX_LEN]) -> Self {
        let mut peer_id = [0; Self::PEER_ID_SIZE];

        let (prefix_segment, suffix_segment) = peer_id.split_at_mut(Self::PREFIX_LEN);
        prefix_segment.copy_from_slice(Self::PEER_ID_VENDOR_PREFIX);

        suffix_segment.copy_from_slice(suffix);

        PeerId(peer_id)
    }

    pub fn random() -> Self {
        let mut rng = rand::thread_rng();
        let suffix = Alphanumeric.sample_string(&mut rng, Self::SUFFIX_LEN);

        Self::new(
            suffix
                .as_bytes()
                .try_into()
                .expect("should not fail as suffix is exactly SUFFIX_LEN long"),
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TrackerRequest {
    /// urlencoded byte representation of the sha1 hash of info.
    pub info_hash: [u8; Self::INFO_HASH_SIZE],

    /// unique peer id string of length 20 bytes.
    pub peer_id: PeerId,

    /// port to listen on
    pub port: u16,

    ///total amount uploaded, start with 0.
    pub uploaded: usize,

    /// total amount downloaded, start with 0
    pub downloaded: usize,

    /// total amount left in the file, set to file size in bytes.
    pub left: usize,

    /// boolean(encoded as a number) for whether to use the
    /// compact reprsentation usually enabled except for backwards compatibility.
    compact: u8,
}

impl TrackerRequest {
    pub const INFO_HASH_SIZE: usize = sha1_smol::DIGEST_LENGTH;

    pub fn new(peer_id: PeerId, port: u16, requestable: &impl Requestable) -> anyhow::Result<Self> {
        Ok(Self {
            info_hash: requestable.get_info_hash()?,
            peer_id,
            port,
            downloaded: 0,
            uploaded: 0,
            left: requestable.get_request_length(),
            compact: 1,
        })
    }
}

pub trait Requestable {
    fn get_info_hash(&self) -> anyhow::Result<[u8; TrackerRequest::INFO_HASH_SIZE]>;
    fn get_request_length(&self) -> usize;
}