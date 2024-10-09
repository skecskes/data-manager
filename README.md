# Rust engineer take away

Subsquid Network is a data lake, that stores and serves queries to blockchain data.

The data is divided into chunks. Each chunk corresponds to particular blockchain and block range,
and stores block related data.

There is a set of workers. Each worker downloads an assigned subset of data chunks from a large persistent storage
and is responsible for serving queries to assigned data.

Your task is to develop the data management component of a worker, that has the following interface:

```rust
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};


pub type DatasetId = [u8; 32];
pub type ChunkId = [u8; 32];


/// data chunk description
pub struct DataChunk {
    id: ChunkId,
    /// Dataset (blockchain) id
    dataset_id: DatasetId,
    /// Block range this chunk is responsible for (around 100 - 10000 blocks)
    block_range: Range<u64>,
    /// Data chunk files. 
    /// A mapping between file names and HTTP URLs to download files from.
    /// Usually contains 1 - 10 files of various sizes. 
    /// The total size of all files in the chunk is about 200 MB.
    files: HashMap<String, String>
}


pub trait DataManager: Send + Sync {
    /// Create a new `DataManager` instance, that will use `data_dir` to store the data.
    /// 
    /// When `data_dir` is not empty, this method should create a list of fully downloaded chunks
    /// and use it as initial state.
    fn new(data_dir: PathBuf) -> Self;
    /// Schedule `chunk` download in background
    fn download_chunk(&self, chunk: DataChunk);
    // List chunks, that are currently available
    fn list_chunks(&self) -> Vec<ChunkId>;
    /// Find a chunk from a given dataset, that is responsible for `block_number`.
    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<impl DataChunkRef>;
    /// Schedule data chunk for deletion in background
    fn delete_chunk(&self, chunk_id: [u8; 32]);
}


// Data chunk must remain available and untouched till this reference is not dropped
pub trait DataChunkRef: Send + Sync + Clone {
    // Data chunk directory
    fn path(&self) -> &Path;
}
```

## Requirements

* `DataManager` functions must not block on IO or hold mutexes for a long time and should be cheap in general
* `DataManager` must be reaching the desired state formed by download-delete call sequence in a fast and smooth manner
* `DataManager` must make no assumptions about download-delete call sequence, except the one below
* You may assume, that at any point of time there is enough disk space to hold the currently desired state
* Only fully downloaded chunks must be available for queries
* Each chunk has approximate size of 200 MB and each worker can store up to 1 TB of data on disk.

## Evaluation

We will not run your code, and you are free to fill in trivial details with `todo!()` macro,
nevertheless, we expect high level structure to be of a production quality and key ideas to be unequivocally expressed.