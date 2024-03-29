use rand::distributions::{Alphanumeric, DistString};
use serde::Serialize;
use urlencoding;

#[derive(Debug, Clone, Serialize)]
#[serde(transparent)]
pub struct PeerId([u8; Self::PEER_ID_SIZE]);

impl AsRef<[u8; Self::PEER_ID_SIZE]> for PeerId {
    fn as_ref(&self) -> &[u8; Self::PEER_ID_SIZE] {
        &self.0
    }
}

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
                .expect("can't fail as suffix is exactly SUFFIX_LEN long"),
        )
    }
}

#[derive(Debug, Clone)]
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
    #[allow(unused)]
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

    pub fn to_url_query(&self) -> String {
        let query_pairs = [
            (
                "info_hash",
                urlencoding::encode_binary(&self.info_hash).to_string(),
            ),
            (
                "peer_id",
                urlencoding::encode_binary(&self.peer_id.as_ref()[..]).to_string(),
            ),
            ("port", self.port.to_string()),
            ("uploaded", self.uploaded.to_string()),
            ("downloaded", self.downloaded.to_string()),
            ("left", self.left.to_string()),
            ("compact", self.compact.to_string()),
        ];
        let mut query_pairs = query_pairs.into_iter();
        // unwrap here should be fine as the query pairs iter's never empty.
        let (first_key, first_val) = query_pairs.next().unwrap();

        query_pairs
            .fold(
                // we don't need to percent encode again as string fields are alphanumeric.
                &mut format!("{}={}", first_key, first_val),
                |output: &mut String, (key, val)| {
                    output.extend(["&", key.as_ref(), "=", val.as_ref()]);
                    output
                },
            )
            .to_string()
    }
}

pub trait Requestable {
    fn get_info_hash(&self) -> anyhow::Result<[u8; TrackerRequest::INFO_HASH_SIZE]>;
    fn get_request_length(&self) -> usize;
}
