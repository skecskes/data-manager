use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use crate::ChunkStatus;
use crate::data_chunk::{ChunkId, DataChunk, DatasetId};

pub struct DataCatalogue {
    pub chunk_ids: Arc<Mutex<HashMap<ChunkId, ChunkStatus>>>,
    chunks_by_id: Arc<Mutex<HashMap<ChunkId, DataChunk>>>,
    chunks_by_dataset: Arc<Mutex<HashMap<DatasetId, HashMap<Range<u64>, DataChunk>>>>,
}

impl DataCatalogue {
    
    pub fn default() -> Self {
        Self::new(Vec::new())
    }
    
    pub fn new(local_chunks_ids: Vec<ChunkId>) -> Self {
        let catalogue = DataCatalogue {
            chunk_ids: Arc::new(Mutex::new(HashMap::new())),
            chunks_by_id: Arc::new(Mutex::new(HashMap::new())),
            chunks_by_dataset: Arc::new(Mutex::new(HashMap::new())),
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
    pub fn get_chunk_id_from_dataset_and_block_range(dataset_id: &DatasetId, block_range: &Range<u64>) -> ChunkId {
        let dataset = hex::encode(dataset_id);
        let chunk_id_str = format!("{}{}{}",dataset, block_range.start, block_range.end);
        let chunk_id_vec = hex::decode(sha256::digest(chunk_id_str.as_bytes())).unwrap();
        let mut chunk_id_array = [0u8; 32];
        chunk_id_array.copy_from_slice(&chunk_id_vec);
        chunk_id_array
    }

    pub fn start_download(&self, chunk: &DataChunk) -> bool {
        {
            if self.chunk_ids.lock().unwrap().contains_key(&chunk.id) {
                return false;
            }
        }
        self.update_chunk(chunk, &ChunkStatus::Downloading);
        true
    }

    pub fn start_deletion(&self, chunk: &DataChunk) -> bool {
        {
            let chunk_ids = self.chunk_ids.lock().unwrap();
            if (
                chunk_ids.contains_key(&chunk.id)
                    && chunk_ids.get(&chunk.id) != Some(&ChunkStatus::Ready)
            ) ||
                chunk_ids.get(&chunk.id).is_none() {
                // don't delete the chunk if it's not ready to be deleted, or it doesn't exist
                return false;
            }
        }
        self.update_chunk(chunk, &ChunkStatus::Deleting);
        true
    }

    pub fn get_ready_chunk_ids(&self) -> Vec<ChunkId> {
        self.chunk_ids.lock().unwrap()
            .iter()
            .filter(|(_, status)| **status == ChunkStatus::Ready)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn update_chunk(&self, chunk: &DataChunk, status: &ChunkStatus) {
        match status {
            ChunkStatus::Downloading => {
                self.chunk_ids.lock().unwrap().insert(chunk.id, ChunkStatus::Downloading);
                self.chunks_by_id.lock().unwrap().remove(&chunk.id);
                self.chunks_by_dataset.lock().unwrap().remove(&chunk.dataset_id);
            },
            ChunkStatus::Ready => {
                self.chunk_ids.lock().unwrap().insert(chunk.id, ChunkStatus::Ready);
                self.chunks_by_id.lock().unwrap().insert(chunk.id, chunk.clone());
                self.chunks_by_dataset.lock().unwrap()
                    .entry(chunk.dataset_id)
                    .or_insert_with(HashMap::new)
                    .insert(chunk.block_range.clone(), chunk.clone());
            },
            ChunkStatus::Deleting => {
                self.chunk_ids.lock().unwrap().insert(chunk.id, ChunkStatus::Deleting);
                self.chunks_by_id.lock().unwrap().remove(&chunk.id);
                self.chunks_by_dataset.lock().unwrap().remove(&chunk.dataset_id);
            },
            ChunkStatus::Deleted => {
                self.chunk_ids.lock().unwrap().remove(&chunk.id);
                self.chunks_by_id.lock().unwrap().remove(&chunk.id);
                self.chunks_by_dataset.lock().unwrap().remove(&chunk.dataset_id);
            },
        }
    }

    pub fn get_chunk_by_id(&self, chunk_id: &ChunkId) -> Option<&DataChunk> {
        self.chunks_by_id.lock().unwrap().get(chunk_id)
    }

    pub fn get_chunk_by_dataset_and_block(&self, dataset_id: &DatasetId, block_number: u64) -> Option<DataChunk> {
        if let Some(chunks) = self.chunks_by_dataset.lock().unwrap().get(dataset_id) {
            for (range, chunk) in chunks {
                if range.contains(&block_number) {
                    return Some(chunk.clone());
                }
            }
        }
        None
    }

}

#[cfg(test)]
mod tests {
    #[test]
    fn test_get_chunk_id_from_dataset_and_block_range() {
        let dataset_id = [0u8; 32];
        let block_range = 0..100;
        let chunk_id = super::DataCatalogue::get_chunk_id_from_dataset_and_block_range(&dataset_id, &block_range);
        assert_eq!(chunk_id.len(), 32);
        assert_eq!(chunk_id, [0u8; 32]);
    }
}