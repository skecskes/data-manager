use std::collections::HashMap;
use std::ops::Range;
use crate::data_chunk::{ChunkId, DataChunk, DatasetId};

pub struct DataCatalogue {
    // TODO: these should be thread safe data structures
    chunks_by_id: HashMap<ChunkId, DataChunk>,
    chunks_by_dataset: HashMap<DatasetId, HashMap<Range<u64>, DataChunk>>,
}

impl DataCatalogue {
    
    pub fn default() -> Self {
        Self::new(Vec::new())
    }
    
    pub fn new(chunks: Vec<DataChunk>) -> Self {
        let mut catalogue = DataCatalogue {
            chunks_by_id: HashMap::new(),
            chunks_by_dataset: HashMap::new(),
        };
        for chunk in chunks {
            catalogue.add_chunk(chunk);
        }
        catalogue
    }

    pub fn add_chunk(&mut self, chunk: DataChunk) {
        self.chunks_by_id.insert(chunk.id, chunk.clone());
        self.chunks_by_dataset
            .entry(chunk.dataset_id)
            .or_insert_with(HashMap::new)
            .insert(chunk.block_range.clone(), chunk);
    }

    pub fn get_chunk_by_id(&self, chunk_id: &ChunkId) -> Option<&DataChunk> {
        self.chunks_by_id.get(chunk_id)
    }

    pub fn get_chunk_by_dataset_and_block(
        &self,
        dataset_id: &DatasetId,
        block_number: u64,
    ) -> Option<&DataChunk> {
        if let Some(chunks) = self.chunks_by_dataset.get(dataset_id) {
            for (range, chunk) in chunks {
                if range.contains(&block_number) {
                    return Some(chunk);
                }
            }
        }
        None
    }

    pub fn delete_chunk(&mut self, chunk_id: &ChunkId) {
        if let Some(chunk) = self.chunks_by_id.remove(chunk_id) {
            self.chunks_by_dataset
                .get_mut(&chunk.dataset_id)
                .map(|chunks| chunks.remove(&chunk.block_range));
        }
    }
}