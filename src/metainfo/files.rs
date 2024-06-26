use crate::torrent::InfoHash;
use crate::tracker::request::Requestable;
use serde::{Deserialize, Serialize};
use sha1_smol::Sha1;

#[derive(Debug, Deserialize, Serialize)]
pub struct File {
    pub path: Vec<String>,
    pub length: usize,

    #[serde(default)]
    pub md5sum: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FileInfo {
    MultiFile {
        #[serde(rename = "name")]
        dirname: String,

        files: Vec<File>,

        #[serde(rename = "piece length")]
        piece_length: usize,

        #[serde(with = "file_hashes_parser")]
        pieces: Vec<FileHash>,

        #[serde(default)]
        private: Option<i64>,
    },

    SingleFile {
        #[serde(rename = "name")]
        filename: String,
        length: usize,

        #[serde(default)]
        md5sum: Option<String>,

        #[serde(rename = "piece length")]
        piece_length: usize,

        #[serde(with = "file_hashes_parser")]
        pieces: Vec<FileHash>,

        #[serde(default)]
        private: Option<i64>,
    },
}

impl Requestable for FileInfo {
    fn get_info_hash(&self) -> anyhow::Result<InfoHash> {
        let info_hash = serde_bencode::to_bytes(self)?;
        Ok(InfoHash::new(Sha1::from(info_hash).digest().bytes()))
    }

    fn get_request_length(&self) -> usize {
        match self {
            Self::SingleFile { length, .. } => *length,
            Self::MultiFile { files, .. } => files.iter().map(|file| file.length).sum(),
        }
    }
}

pub type FileHash = [u8; 20];

mod file_hashes_parser {
    use super::FileHash;
    use serde::de::{self, Visitor};
    use static_str_ops::static_format;
    const HASH_SIZE: usize = std::mem::size_of::<FileHash>();

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<FileHash>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(FileHashVisitor)
    }

    pub fn serialize<S>(
        file_hashes: impl AsRef<[FileHash]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde_bytes::serialize(&file_hashes.as_ref().concat(), serializer)
    }

    struct FileHashVisitor;
    impl<'de> Visitor<'de> for FileHashVisitor {
        type Value = Vec<FileHash>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str(static_format!(
                "a byte sequence whose length is a multiple of {}",
                HASH_SIZE
            ))
        }

        fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let n_bytes = bytes.len();

            if n_bytes % HASH_SIZE != 0 {
                return Err(E::custom(static_format!(
                    "file hash pieces should be a multiple of length {}",
                    HASH_SIZE
                )));
            }

            //TODO: use array_chunks::<20> instead of chunks_exact when it becomes stable.
            let file_hash_slices = bytes
                .chunks_exact(HASH_SIZE)
                .map(|chunk| {
                    chunk.try_into().expect(static_format!(
                        "chunks_exact returns only chunks which are length {}",
                        HASH_SIZE
                    ))
                })
                .collect();

            Ok(file_hash_slices)
        }
    }
}
