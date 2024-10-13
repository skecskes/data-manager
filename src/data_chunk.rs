use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;

pub type DatasetId = [u8; 32];
pub type ChunkId = [u8; 32];


/// data chunk description
#[derive(Clone)]
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

// Data chunk must remain available and untouched till this reference is not dropped
pub trait DataChunkRef: Send + Sync + Clone {
    // Data chunk directory
    fn path(&self) -> &Path;
}

impl DataChunkRef for DataChunk {
    fn path(&self) -> &Path {
        // TODO: this is wrong, but we will fix it later. It should be a path to the directory
        &self.files.iter().take(1).next().unwrap().0.as_ref()
    }
}
