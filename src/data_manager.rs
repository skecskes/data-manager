use std::path::{Path, PathBuf};
use crate::data_chunk::{ChunkId, DataChunk, DatasetId};

pub trait DataManager: Send + Sync {
    /// Create a new `DataManager` instance, that will use `data_dir` to store the data.
    ///
    /// When `data_dir` is not empty, this method should create a list of fully downloaded chunks
    /// and use it as initial state.
    fn new(data_dir: PathBuf) -> Self;

    /// Schedule `chunk` download in background
    fn download_chunk(&self, chunk: DataChunk);

    /// List chunks, that are currently available
    fn list_chunks(&self) -> Vec<ChunkId>;

    /// Find a chunk from a given dataset, that is responsible for `block_number`.
    fn find_chunk(&self, dataset_id: DatasetId, block_number: u64) -> Option<impl DataChunkRef>;

    /// Schedule data chunk for deletion in background
    fn delete_chunk(&self, chunk_id: ChunkId);
}


// Data chunk must remain available and untouched till this reference is not dropped
pub trait DataChunkRef: Send + Sync + Clone {
    // Data chunk directory
    fn path(&self) -> &Path;
}

impl DataChunkRef for DataChunk {
    fn path(&self) -> &Path {
        unimplemented!()
    }
}