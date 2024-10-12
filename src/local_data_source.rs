use crate::data_chunk::{ChunkId, DataChunk};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, thread};
use std::collections::HashMap;
use crate::ChunkStatus;

#[derive(Clone)]
pub struct LocalDataSource {
    pub data_dir: PathBuf,
}

impl LocalDataSource {
    pub fn new(data_dir: PathBuf) -> Self {
        LocalDataSource { data_dir }
    }

    pub fn list_existing_chunk_ids(&self) -> HashMap<ChunkId, ChunkStatus> {
        let mut chunk_ids = HashMap::new();

        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                if let Ok(file_name) = entry.file_name().into_string() {
                    if file_name.len() == 64 {
                        if let Ok(chunk_id) = hex::decode(file_name) {
                            if chunk_id.len() == 32 {
                                let mut chunk_id_array = [0u8; 32];
                                chunk_id_array.copy_from_slice(&chunk_id);
                                chunk_ids.insert(chunk_id_array, ChunkStatus::Ready);
                            }
                        }
                    }
                }
            }
        }

        chunk_ids
    }

    /// Download the all the chunks to the data_dir as one chunk_id file
    pub fn download_chunk(data_dir: PathBuf, chunk: DataChunk) -> String {
        
        // Simulate downloading the chunk by waiting for 100ms
        thread::sleep(Duration::from_millis(100));
        let chunk_id = hex::encode(chunk.id);
        // create file with name chunk_id in data_dir
        let file_path = data_dir.join(&chunk_id);
        fs::write(file_path, b"").expect("Failed to write file");
        
        format!(
            "Downloading the chunk {} to {} has completed",
            chunk_id,
            data_dir.display()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instantiate_local_data_source() {
        let ds = LocalDataSource::new(PathBuf::from("./test_data_dir"));
        assert_eq!(ds.data_dir, PathBuf::from("./test_data_dir"));
    }

    #[test]
    fn test_list_files_as_chunk_ids() {
        let ds = LocalDataSource::new(PathBuf::from("./test_data_dir"));
        let chunk_ids = ds.list_existing_chunk_ids();
        assert_eq!(chunk_ids.len(), 4);
        assert!(chunk_ids.contains_key(&[0u8; 32]));
        assert_eq!(chunk_ids.get(&[0u8; 32]), Some(&ChunkStatus::Ready));
        
        assert!(chunk_ids.contains_key(&[17; 32]));
        assert_eq!(chunk_ids.get(&[17; 32]), Some(&ChunkStatus::Ready));
        assert_eq!(
            chunk_ids.get(&[
                123, 94, 164, 195, 214, 231, 248, 169, 176, 193, 210, 227, 244, 165, 182, 199, 216,
                233, 240, 161, 178, 195, 212, 229, 246, 167, 184, 201, 208, 225, 242, 163
            ]
        ), Some(&ChunkStatus::Ready));
        assert_eq!(
            chunk_ids.get(&[
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31
            ]
        ), Some(&ChunkStatus::Ready));
    }
    
    #[test]
    fn test_download_chunk() {
        // Arrange
        let ds = LocalDataSource::new(PathBuf::from("./test_data_dir"));
        let chunk = DataChunk {
            id: [5u8; 32],
            dataset_id: [0u8; 32],
            block_range: 0..0,
            files: Default::default(),
        };
        
        // Act
        let result = LocalDataSource::download_chunk(ds.data_dir.clone(), chunk.clone());
        assert_eq!(
            result,
            "Downloading the chunk 0505050505050505050505050505050505050505050505050505050505050505 to ./test_data_dir has completed"
        );
        
        // Assert
        let chunk_ids = ds.list_existing_chunk_ids();
        assert_eq!(chunk_ids.len(), 5);
        
        let chunk_id = hex::encode(chunk.id);
        assert!(chunk_ids.contains_key(&chunk.id));
        assert_eq!(chunk_ids.get(&chunk.id), Some(&ChunkStatus::Ready));
        
        let file_path = ds.data_dir.join(&chunk_id);
        assert!(file_path.exists());
        
        fs::remove_file(file_path).expect("Failed to remove file");
    }
}
