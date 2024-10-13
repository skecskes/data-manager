use crate::data_chunk::{ChunkId, DataChunk};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, thread};
use crate::ChunkStatus;
use crate::data_catalogue::DataCatalogue;

#[derive(Clone)]
pub struct LocalDataSource {
    pub data_dir: PathBuf,
}

impl LocalDataSource {
    pub fn new(data_dir: PathBuf) -> Self {
        LocalDataSource { data_dir }
    }
    
    pub fn read_local_chunks(&self) -> Vec<ChunkId> {
        let mut chunks = Vec::new();
       
        // chunk id is concatenated dataset_id and block_range hashed with sha256 into [u8; 32]
        
        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                if let Ok(main_directory) = entry.file_name().into_string() {
                    if main_directory.contains("=") {
                        for dataset_directory in entry.path().read_dir().unwrap() {
                            if let Ok(block_range_directory) = dataset_directory.unwrap().file_name().into_string() {
                                if block_range_directory.contains("=") {
                                    let dataset = hex::decode(main_directory.split("=").collect::<Vec<&str>>()[1]).unwrap();
                                    let dataset_id = dataset.as_slice().try_into().unwrap();
                                    let block_range = block_range_directory.split("=").collect::<Vec<&str>>()[1];
                                    let block_start = block_range.split("_").collect::<Vec<&str>>()[0].parse::<u64>().unwrap();
                                    let block_end = block_range.split("_").collect::<Vec<&str>>()[1].parse::<u64>().unwrap();
                                    
                                    let chunk_id = DataCatalogue::get_chunk_id_from_dataset_and_block_range(dataset_id, block_start..block_end);
                                    chunks.push(chunk_id);
                                }
                            }
                            
                        }
                    }
                }
            }
        }
        
        
        chunks
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
    
    pub fn delete_chunk(data_dir: PathBuf, chunk_id: ChunkId) -> String {
        // Simulate deleting the chunk by waiting for 100ms
        thread::sleep(Duration::from_millis(100));
        let chunk_id = hex::encode(chunk_id);
        let file_path = data_dir.join(&chunk_id);
        fs::remove_file(file_path).expect("Failed to remove file");
        format!("Deleting the chunk {} from {} has completed", chunk_id, data_dir.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instantiate_local_data_source() {
        let ds = LocalDataSource::new(PathBuf::from("./local_data_dir"));
        assert_eq!(ds.data_dir, PathBuf::from("./local_data_dir"));
    }

    #[test]
    fn test_list_files_as_chunk_ids() {
        // Act
        let ds = LocalDataSource::new(PathBuf::from("./local_data_dir"));
        let chunk_ids = ds.read_local_chunks();
        
        // Assert
        assert_eq!(chunk_ids.len(), 8);
        assert_eq!(chunk_ids[0], [147, 161, 202, 94, 141, 129, 235, 161, 211, 123, 214, 159, 212, 119, 7, 59, 107, 144, 48, 224, 108, 245, 142, 139, 2, 173, 240, 231, 54, 58, 115, 159]);
        assert_eq!(chunk_ids[1], [52, 249, 87, 41, 193, 143, 108, 194, 169, 137, 151, 250, 99, 44, 49, 211, 165, 208, 160, 65, 58, 31, 238, 223, 208, 29, 143, 142, 93, 6, 220, 211]);
        assert_eq!(chunk_ids[2], [193, 57, 118, 234, 133, 186, 129, 98, 68, 9, 137, 174, 130, 138, 250, 203, 200, 19, 226, 101, 224, 108, 235, 80, 186, 6, 49, 14, 23, 58, 108, 70]);
        assert_eq!(chunk_ids[3], [118, 166, 206, 104, 188, 72, 255, 213, 176, 59, 193, 246, 55, 235, 118, 138, 87, 148, 244, 77, 58, 207, 103, 229, 97, 58, 212, 176, 79, 143, 187, 4]);
        assert_eq!(chunk_ids[4], [98, 39, 185, 12, 229, 13, 3, 121, 220, 39, 48, 2, 38, 129, 54, 147, 17, 92, 89, 191, 47, 125, 227, 35, 162, 83, 99, 140, 124, 47, 92, 153]);
        assert_eq!(chunk_ids[5], [47, 214, 124, 127, 237, 100, 240, 96, 40, 147, 96, 68, 104, 154, 218, 127, 165, 181, 128, 44, 47, 16, 60, 172, 24, 208, 88, 136, 149, 79, 243, 191]);
        assert_eq!(chunk_ids[6], [56, 24, 248, 27, 82, 241, 162, 191, 1, 219, 253, 77, 160, 250, 121, 88, 143, 116, 109, 77, 123, 216, 197, 83, 201, 51, 240, 120, 186, 231, 249, 76]);
        assert_eq!(chunk_ids[7], [168, 77, 161, 67, 100, 46, 30, 66, 3, 236, 122, 88, 18, 185, 131, 120, 153, 130, 152, 113, 236, 29, 91, 3, 244, 6, 254, 177, 61, 66, 182, 178]);
        
    }
    
    #[test]
    fn test_download_chunk() {
        // Arrange
        let ds = LocalDataSource::new(PathBuf::from("./local_data_dir"));
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
            "Downloading the chunk 0505050505050505050505050505050505050505050505050505050505050505 to ./local_data_dir has completed"
        );
        
        // Assert
        let chunk_ids = ds.read_local_chunks();
        assert_eq!(chunk_ids.len(), 5);
        
        let chunk_id = hex::encode(chunk.id);
        assert!(chunk_ids.contains_key(&chunk.id));
        assert_eq!(chunk_ids.get(&chunk.id), Some(&ChunkStatus::Ready));
        
        let file_path = ds.data_dir.join(&chunk_id);
        assert!(file_path.exists());
        
        fs::remove_file(file_path).expect("Failed to remove file");
    }
}
