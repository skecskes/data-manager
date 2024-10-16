use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};
use crate::data_chunk::{ChunkId, DataChunk, DatasetId};
use polars::prelude::*;
use crate::local_data_source::{LocalDataSource, LOCAL_DATA_DIR};

const LOCAL_CATALOGUE: &str = "./local_catalogue_dir/registry.parquet";

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkStatus {
    Downloading,
    Ready,
    Deleting,
    Deleted,
}

impl ToString for ChunkStatus {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Clone, Debug)]
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

    pub fn new(local_chunks: Vec<DataChunk>) -> Self {
        let catalogue = DataCatalogue {
            registry: Arc::new(RwLock::new(HashMap::new())),
        };

        // load local chunks into the registry
        let db_chunk_infos = DataCatalogue::read_parquet_to_chunks(LOCAL_CATALOGUE);
        for local_chunk in local_chunks.iter() {

            // data integrity check and update
            if db_chunk_infos.iter().any(|db_chunk_info| {
                db_chunk_info.chunk.id == local_chunk.id && db_chunk_info.status != ChunkStatus::Ready
            }) {
                continue;
            }
            catalogue.registry.write().unwrap().insert(local_chunk.id, ChunkInfo {
                chunk: local_chunk.clone(),
                status: ChunkStatus::Ready,
            });
        }
        catalogue
    }

    /// This function generates a unique chunk id from the dataset id and block range
    /// Note: To be used everywhere to have consistent chunk ids!!!
    pub fn generate_chunk_id(dataset_id: &DatasetId, block_range: &Range<u64>) -> ChunkId {
        let dataset = hex::encode(dataset_id);
        let chunk_id_str = format!("{}{}{}", dataset, block_range.start, block_range.end);
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
        {
            let mut registry = self.registry.write().unwrap();
            match status {
                ChunkStatus::Downloading => {
                    registry.insert(chunk.id, ChunkInfo {
                        chunk: chunk.clone(),
                        status: ChunkStatus::Downloading,
                    });
                }
                ChunkStatus::Ready => {
                    registry.insert(chunk.id, ChunkInfo {
                        chunk: chunk.clone(),
                        status: ChunkStatus::Ready,
                    });
                }
                ChunkStatus::Deleting => {
                    registry.insert(chunk.id, ChunkInfo {
                        chunk: chunk.clone(),
                        status: ChunkStatus::Deleting,
                    });
                }
                ChunkStatus::Deleted => {
                    registry.insert(chunk.id, ChunkInfo {
                        chunk: chunk.clone(),
                        status: ChunkStatus::Deleted,
                    });
                }
            }
        }
        let chunk_infos = self.registry.read().unwrap().values().map(|info| info.clone()).collect::<Vec<ChunkInfo>>();
        DataCatalogue::save_chunk_infos_to_parquet(&chunk_infos, LOCAL_CATALOGUE);
    }

    pub fn get_chunk_by_id(&self, chunk_id: &ChunkId) -> Option<DataChunk> {
        self.registry.read().unwrap().get(chunk_id).map(|info| info.chunk.clone())
    }

    pub fn find_chunk(&self, dataset_id: &DatasetId, block_number: u64) -> Option<DataChunk> {
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

    fn save_chunk_infos_to_parquet(chunk_infos: &[ChunkInfo], file_path: &str) {
        let mut df = DataCatalogue::chunk_infos_to_dataframe(chunk_infos);

        let writer = std::fs::File::create(file_path).unwrap();
        let p_writer = ParquetWriter::new(writer);
        p_writer.finish(&mut df).unwrap();
    }

    fn read_parquet_to_chunks(file_path: &str) -> Vec<ChunkInfo> {
        let reader = std::fs::File::open(file_path).unwrap();
        let p_reader = ParquetReader::new(reader);
        let df = p_reader.finish().unwrap();

        DataCatalogue::dataframe_to_chunk_infos(df)
    }

    fn dataframe_to_chunk_infos(df: DataFrame) -> Vec<ChunkInfo> {
        let id = df.column("id").unwrap().str().unwrap();
        let dataset_id = df.column("dataset_id").unwrap().str().unwrap();
        let block_form = df.column("block_form").unwrap().as_any().downcast_ref::<UInt64Chunked>().unwrap();
        let block_to = df.column("block_to").unwrap().as_any().downcast_ref::<UInt64Chunked>().unwrap();
        let files = df.column("files").unwrap().str().unwrap();
        let status = df.column("status").unwrap().str().unwrap();
        (0..df.height())
            .map(|i| {
                ChunkInfo {
                    chunk: DataChunk {
                        id: hex::decode(id.get(i).unwrap()).unwrap().try_into().unwrap(),
                        dataset_id: hex::decode(dataset_id.get(i).unwrap()).unwrap().try_into().unwrap(),
                        block_range: block_form.get(i).unwrap()..block_to.get(i).unwrap(),
                        files: serde_json::from_str(files.get(i).unwrap()).unwrap(),
                    },
                    status: match status.get(i).unwrap() {
                        "Downloading" => ChunkStatus::Downloading,
                        "Ready" => ChunkStatus::Ready,
                        "Deleting" => ChunkStatus::Deleting,
                        _ => ChunkStatus::Deleted,
                    }
                }
            }).collect()
    }

    fn chunk_infos_to_dataframe(chunks: &[ChunkInfo]) -> DataFrame {
        df!(
            "id" => chunks.iter().map(|x| hex::encode(x.chunk.id)).collect::<Vec<String>>(),
            "dataset_id" => chunks.iter().map(|x| hex::encode(x.chunk.dataset_id)).collect::<Vec<String>>(),
            "block_form" => chunks.iter().map(|x| x.chunk.block_range.start).collect::<Vec<u64>>(),
            "block_to" => chunks.iter().map(|x| x.chunk.block_range.end).collect::<Vec<u64>>(),
            "files" => chunks.iter().map(|x| serde_json::to_string(&x.chunk.files).unwrap()).collect::<Vec<String>>(),
            "status" => chunks.iter().map(|x| x.status.to_string()).collect::<Vec<String>>()
        ).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use crate::DataCatalogue;
    use crate::data_catalogue::{ChunkInfo, LOCAL_CATALOGUE};
    use crate::local_data_source::{LocalDataSource, LOCAL_DATA_DIR};

    #[test]
    fn test_get_chunk_id_from_dataset_and_block_range() {
        let dataset_id = [0u8; 32];
        let block_range = 0..100;
        let chunk_id = DataCatalogue::generate_chunk_id(&dataset_id, &block_range);
        assert_eq!(chunk_id.len(), 32);
        assert_eq!(chunk_id, [136, 104, 208, 73, 150, 126, 253, 47, 9, 128, 63, 93, 28, 177, 175, 129, 70, 243, 137, 65, 94, 32, 62, 196, 150, 239, 161, 94, 122, 237, 101, 81]);
    }

    #[test]
    #[serial]
    fn test_instantiated_catalogue_exists() {
        let catalogue = DataCatalogue::default();
        assert_eq!(catalogue.registry.read().unwrap().len(), 0);
    }

    #[test]
    #[serial]
    fn test_saving_registry_in_db() {
        // Arrange
        std::fs::remove_file(LOCAL_CATALOGUE).unwrap();
        let data_source = LocalDataSource::new(LOCAL_DATA_DIR.into());
        let chunk_infos = data_source.get_local_chunks().iter().map(|chunk| ChunkInfo {
            chunk: chunk.clone(),
            status: super::ChunkStatus::Ready,
        }).collect::<Vec<ChunkInfo>>();

        // Act
        DataCatalogue::save_chunk_infos_to_parquet(&chunk_infos, LOCAL_CATALOGUE);

        // Assert file exists in LOCAL_CATALOGUE
        assert_eq!(std::path::Path::new(LOCAL_CATALOGUE).exists(), true);
    }

    #[test]
    #[serial]
    fn test_reading_registry_from_db() {
        // Arrange
        std::fs::remove_file(LOCAL_CATALOGUE).unwrap();
        let data_source = LocalDataSource::new(LOCAL_DATA_DIR.into());
        let chunk_infos = data_source.get_local_chunks().iter().map(|chunk| ChunkInfo {
            chunk: chunk.clone(),
            status: super::ChunkStatus::Ready,
        }).collect::<Vec<ChunkInfo>>();
        DataCatalogue::save_chunk_infos_to_parquet(&chunk_infos, LOCAL_CATALOGUE);

        // Act
        let actual = DataCatalogue::read_parquet_to_chunks(LOCAL_CATALOGUE);

        // Assert
        assert_eq!(actual.len(), 8);
        assert_eq!(actual[0].chunk.id, [147, 161, 202, 94, 141, 129, 235, 161, 211, 123, 214, 159, 212, 119, 7, 59, 107, 144, 48, 224, 108, 245, 142, 139, 2, 173, 240, 231, 54, 58, 115, 159]);
        assert_eq!(actual[0].chunk.dataset_id, [17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17]);
        assert_eq!(actual[0].chunk.block_range, 0..35);
        assert_eq!(actual[0].chunk.files.len(), 3);
        assert_eq!(actual[0].chunk.files.get("part-1.parquet").unwrap(), "./local_data_dir/dataset_id=1111111111111111111111111111111111111111111111111111111111111111/block_range=0_35/part-1.parquet");
        assert_eq!(actual[0].status, super::ChunkStatus::Ready);
    }
}

pub(crate) fn load_catalogue_with_local_chunks() {
    // cleanup
    let data_source = LocalDataSource::new(LOCAL_DATA_DIR.into());
    let chunks = data_source.get_local_chunks();
    let chunk_infos = chunks.iter().map(|chunk| ChunkInfo {
        chunk: chunk.clone(),
        status: ChunkStatus::Ready,
    }).collect::<Vec<ChunkInfo>>();
    DataCatalogue::save_chunk_infos_to_parquet(&chunk_infos, LOCAL_CATALOGUE);
}
