use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use crate::local_data_source::LOCAL_DATA_DIR;

pub type DatasetId = [u8; 32];
pub type ChunkId = [u8; 32];


/// data chunk description
#[derive(Clone, Debug, PartialEq)]
pub struct DataChunk {
    pub id: ChunkId,
    /// Dataset (blockchain) id
    pub dataset_id: DatasetId,
    /// Block range this chunk is responsible for (around 100 - 10000 blocks)
    pub block_range: Range<u64>,
    /// Data chunk files.
    /// A mapping between file names and HTTP URLs to download files from.
    /// Usually contains 1 - 10 files of various sizes.
    /// The total size of all files in the chunk is about 200 MB.
    pub files: HashMap<String, String>
}

/// Data chunk path
#[derive(Clone, Debug, PartialEq)]
pub struct DataChunkPath {
    pub chunk: DataChunk,
    pub path: PathBuf,
}

impl DataChunkPath {
    pub fn new(chunk: DataChunk) -> Self {
        let path = PathBuf::from(format!(
            "{}/dataset_id={}/block_range={}_{}/",
            LOCAL_DATA_DIR,
            hex::encode(chunk.dataset_id),
            chunk.block_range.start,
            chunk.block_range.end
        ));
        DataChunkPath { chunk, path }
    }
}


// Data chunk must remain available and untouched till this reference is not dropped
pub trait DataChunkRef: Send + Sync + Clone {
    // Data chunk directory
    fn path(&self) -> &Path;
}

impl DataChunkRef for DataChunkPath {
    fn path(&self) -> &Path {
        &self.path
    }
}
