use std::path::PathBuf;
use crate::data_chunk::{ChunkId, DataChunk};
use crate::data_manager::DataManager;
use crate::local_data_source::LocalDataSource;

pub mod data_chunk;
mod data_manager;
mod local_data_source;

pub struct DataManagerImpl {
    chunk_ids: Vec<ChunkId>
}

impl DataManager for DataManagerImpl {
    fn new(data_dir: PathBuf) -> Self {
        let data_source = LocalDataSource::new(data_dir);
        let chunk_ids = data_source.list_files_as_chunk_ids();
        DataManagerImpl {
            chunk_ids
        }
    }

    fn download_chunk(&self, chunk: DataChunk) {
        /// Schedule `chunk` download in background
        unimplemented!()
    }

    fn list_chunks(&self) -> Vec<ChunkId> {
        self.chunk_ids.clone()
    }

    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<DataChunk> {
        unimplemented!()
    }

    fn delete_chunk(&self, chunk_id: [u8; 32]) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instantiate_data_manager() {
        let dm = DataManagerImpl::new(PathBuf::from("./test_data_dir"));
        assert_eq!(dm.chunk_ids.len(), 4);
        assert_eq!(dm.chunk_ids[0], [0u8; 32]);
    }

    #[test]
    fn test_list_chunks() {
        let dm = DataManagerImpl::new(PathBuf::from("./test_data_dir"));
        let chunk_ids = dm.list_chunks();
        assert_eq!(chunk_ids.len(), 4);
    }
}



