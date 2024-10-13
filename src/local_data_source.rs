use crate::data_chunk::{ChunkId, DataChunk};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fs, thread};
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
                                    let dataset_id_str = main_directory.split("=").nth(1).unwrap();
                                    let dataset_id_vec = hex::decode(dataset_id_str).unwrap();
                                    let mut dataset_id = [0u8; 32];
                                    dataset_id.copy_from_slice(&dataset_id_vec);

                                    let block_range = block_range_directory.split("=").nth(1).unwrap();
                                    let mut parts = block_range.split('_');
                                    let block_start = parts.next().unwrap().parse::<u64>().unwrap();
                                    let block_end = parts.next().unwrap().parse::<u64>().unwrap();
                                    let range = block_start..block_end;
                                    let chunk_id = DataCatalogue::get_chunk_id_from_dataset_and_block_range(&dataset_id, &range);
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
        simulate_downloading_chunk(data_dir.clone(), chunk.clone());
        format!(
            "Downloading the chunk {:?} to {} has completed",
            chunk.id,
            data_dir.display()
        )
    }
    
    pub fn delete_chunk(data_dir: PathBuf, chunk_id: ChunkId) -> String {
        // Simulate deleting the chunk by waiting for 100ms
        simulate_deleting_chunk(&data_dir, &chunk_id);

        format!("Deleting the chunk {:?} from {} has completed", chunk_id, data_dir.display())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
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
        let dataset_id_str = "1111111111111111111111111111111111111111111111111111111111111111";
        let dataset_id_vec = hex::decode(dataset_id_str).unwrap();
        let mut dataset_id = [0u8; 32];
        dataset_id.copy_from_slice(&dataset_id_vec);

        let block_range = 95..106;
        let chunk_id = DataCatalogue::get_chunk_id_from_dataset_and_block_range(&dataset_id, &block_range);
        let chunk = DataChunk {
            id: chunk_id,
            dataset_id: dataset_id,
            block_range: block_range,
            files: HashMap::from([
                ("part-1.parquet".to_string(), "https://example.com/par-1.parquet".to_string()),
                ("part-2.parquet".to_string(), "https://example.com/par-2.parquet".to_string()),
                ("part-3.parquet".to_string(), "https://example.com/par-3.parquet".to_string()),
            ]),
        };

        // Act
        let result = LocalDataSource::download_chunk(ds.data_dir.clone(), chunk.clone());

        // Assert
        assert_eq!(
            result,
            "Downloading the chunk [170, 13, 118, 225, 28, 2, 234, 149, 141, 239, 145, 9, 120, 116, 116, 137, 16, 29, 106, 129, 18, 70, 73, 152, 183, 85, 25, 49, 33, 116, 247, 65] to ./local_data_dir has completed"
        );

        let chunk_ids = ds.read_local_chunks();
        assert_eq!(chunk_ids.len(), 9);

        assert!(chunk_ids.contains(&chunk.id));

        simulate_deleting_chunk(&ds.data_dir.clone(), &chunk.id);
    }

    #[test]
    fn test_delete_chunk() {
        // Arrange
        let ds = LocalDataSource::new(PathBuf::from("./local_data_dir"));
        let dataset_id_str = "1111111111111111111111111111111111111111111111111111111111111111";
        let dataset_id_vec = hex::decode(dataset_id_str).unwrap();
        let mut dataset_id = [1u8; 32];
        dataset_id.copy_from_slice(&dataset_id_vec);

        let block_range = 95..106;
        let chunk_id = DataCatalogue::get_chunk_id_from_dataset_and_block_range(&dataset_id, &block_range);
        let chunk = DataChunk {
            id: chunk_id,
            dataset_id: dataset_id,
            block_range: block_range,
            files: HashMap::from([
                ("part-1.parquet".to_string(), "https://example.com/par-1.parquet".to_string()),
                ("part-2.parquet".to_string(), "https://example.com/par-2.parquet".to_string()),
                ("part-3.parquet".to_string(), "https://example.com/par-3.parquet".to_string()),
            ]),
        };
        simulate_downloading_chunk(ds.data_dir.clone(), chunk.clone());
        let chunk_ids = ds.read_local_chunks();
        assert_eq!(chunk_ids.len(), 9);
        assert!(chunk_ids.contains(&chunk.id));

        // Act
        let result = LocalDataSource::delete_chunk(ds.data_dir.clone(), chunk.id);

        // Assert
        assert_eq!(
            result,
            "Deleting the chunk [170, 13, 118, 225, 28, 2, 234, 149, 141, 239, 145, 9, 120, 116, 116, 137, 16, 29, 106, 129, 18, 70, 73, 152, 183, 85, 25, 49, 33, 116, 247, 65] from ./local_data_dir has completed"
        );

        let chunk_ids = ds.read_local_chunks();
        assert_eq!(chunk_ids.len(), 8);
        assert!(!chunk_ids.contains(&chunk.id));
    }
}

fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
    if !dst.exists() {
        fs::create_dir(dst)?;
    }
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Simulate downloading the chunk taking 100ms
fn simulate_downloading_chunk(data_dir: PathBuf, chunk: DataChunk) {
    thread::sleep(Duration::from_millis(100));
    if chunk.dataset_id == [17u8; 32] && chunk.block_range.start == 95 && chunk.block_range.end == 106 {
        copy_dir_all(
            Path::new("./remote_data_dir/dataset_id=1111111111111111111111111111111111111111111111111111111111111111/block_range=95_106"),
            Path::new(&format!("{}/dataset_id=1111111111111111111111111111111111111111111111111111111111111111/block_range=95_106", data_dir.display()))
        ).expect("Failed to copy directory");
    };
}

/// Simulate deleting the chunk taking 100ms
fn simulate_deleting_chunk(data_dir: &PathBuf, chunk_id: &ChunkId) {
    thread::sleep(Duration::from_millis(100));
    if chunk_id.eq(&[170, 13, 118, 225, 28, 2, 234, 149, 141, 239, 145, 9, 120, 116, 116, 137, 16, 29, 106, 129, 18, 70, 73, 152, 183, 85, 25, 49, 33, 116, 247, 65]) {
        fs::remove_dir_all(
                      Path::new(&format!("{}/dataset_id=1111111111111111111111111111111111111111111111111111111111111111/block_range=95_106", data_dir.display()))
        ).expect("Failed to remove directory");
    };
}