use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};
use crate::data_chunk::{ChunkId, DataChunk, DataChunkRef, DatasetId};

#[derive(Debug, Clone)]
#[derive(PartialEq)]
pub enum ChunkStatus {
    Downloading,
    Ready,
    Deleting,
    Deleted,
}

pub struct ChunkInfo {
    pub chunk: DataChunk,
    pub status: ChunkStatus,
}

#[derive(Clone)]
pub struct DataCatalogue {
    pub registry: Arc<RwLock<HashMap<ChunkId, ChunkInfo>>>,
}

impl DataCatalogue {
    
    pub fn default() -> Self {
        Self::new(Vec::new())
    }
    
    pub fn new(local_chunks_ids: Vec<ChunkId>) -> Self {
        let catalogue = DataCatalogue {
            registry: Arc::new(RwLock::new(HashMap::new())),
        };
        for chunk_id in local_chunks_ids {
            // TODO: get full chunk details from the database
            // fill missing data and instantiate properly
            catalogue.update_chunk(&DataChunk {
                id: chunk_id,
                dataset_id: [0u8; 32],
                block_range: 0..0,
                files: HashMap::new(),
            }, &ChunkStatus::Ready);
        }
        catalogue
    }

    /// This function generates a unique chunk id from the dataset id and block range
    /// Note: To be used everywhere to have consistent chunk ids!!!
    pub fn generate_chunk_id(dataset_id: &DatasetId, block_range: &Range<u64>) -> ChunkId {
        let dataset = hex::encode(dataset_id);
        let chunk_id_str = format!("{}{}{}",dataset, block_range.start, block_range.end);
        let chunk_id_vec = hex::decode(sha256::digest(chunk_id_str.as_bytes())).unwrap();
        let mut chunk_id_array = [0u8; 32];
        chunk_id_array.copy_from_slice(&chunk_id_vec);
        chunk_id_array
    }

    pub fn start_download(&self, chunk: &DataChunk) -> bool {
        {
            let registry = self.registry.read().unwrap();
            if registry.contains_key(&chunk.id) && registry.get(&chunk.id).unwrap().status != ChunkStatus::Deleted
            || !registry.get(&chunk.id).is_none() {
                // don't download the chunk if it's already being downloaded, or it's not deleted
                return false;
            }
        }
        self.update_chunk(chunk, &ChunkStatus::Downloading);
        true
    }

    pub fn start_deletion(&self, chunk: &DataChunk) -> bool {
        {
            let registry = self.registry.read().unwrap();
            if (registry.contains_key(&chunk.id) && registry.get(&chunk.id).unwrap().status != ChunkStatus::Ready)
                || registry.get(&chunk.id).is_none() {
                // don't delete the chunk if it's not ready to be deleted, or it doesn't exist
                return false;
            }
        }
        self.update_chunk(chunk, &ChunkStatus::Deleting);
        true
    }

    pub fn get_ready_chunk_ids(&self) -> Vec<ChunkId> {
        self.registry.read().unwrap()
            .iter()
            .filter(|(_, info)| info.status == ChunkStatus::Ready)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn update_chunk(&self, chunk: &DataChunk, status: &ChunkStatus) {
        let mut registry = self.registry.write().unwrap();
        match status {
            ChunkStatus::Downloading => {
                registry.insert(chunk.id, ChunkInfo {
                    chunk: chunk.clone(),
                    status: ChunkStatus::Downloading,
                });
            },
            ChunkStatus::Ready => {
                registry.insert(chunk.id, ChunkInfo {
                    chunk: chunk.clone(),
                    status: ChunkStatus::Ready,
                });
            },
            ChunkStatus::Deleting => {
                registry.insert(chunk.id, ChunkInfo {
                    chunk: chunk.clone(),
                    status: ChunkStatus::Deleting,
                });
            },
            ChunkStatus::Deleted => {
                registry.insert(chunk.id, ChunkInfo {
                    chunk: chunk.clone(),
                    status: ChunkStatus::Deleted,
                });
            },
        }
    }

    pub fn get_chunk_by_id(&self, chunk_id: &ChunkId) -> Option<DataChunk> {
        self.registry.read().unwrap().get(chunk_id).map(|info| info.chunk.clone())
    }

    pub fn find_chunk(&self, dataset_id: &DatasetId, block_number: u64) -> Option<impl DataChunkRef> {
        self.registry.read().unwrap().values()
            .find(|info|
                {
                    info.chunk.dataset_id == *dataset_id
                    && info.chunk.block_range.contains(&block_number)
                    && info.status == ChunkStatus::Ready
                }
            )
            .map(|info| info.chunk.clone())
    }

}

#[cfg(test)]
mod tests {
    #[test]
    fn test_get_chunk_id_from_dataset_and_block_range() {
        let dataset_id = [0u8; 32];
        let block_range = 0..100;
        let chunk_id = super::DataCatalogue::generate_chunk_id(&dataset_id, &block_range);
        assert_eq!(chunk_id.len(), 32);
        assert_eq!(chunk_id, [136, 104, 208, 73, 150, 126, 253, 47, 9, 128, 63, 93, 28, 177, 175, 129, 70, 243, 137, 65, 94, 32, 62, 196, 150, 239, 161, 94, 122, 237, 101, 81]);
    }
}