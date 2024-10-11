use std::fs;
use std::path::{PathBuf};
use crate::data_chunk::ChunkId;

pub struct LocalDataSource {
    data_dir: PathBuf,
}

impl LocalDataSource {
    pub fn new(data_dir: PathBuf) -> Self {
        LocalDataSource {
            data_dir
        }
    }

    pub fn list_files_as_chunk_ids(&self) -> Vec<ChunkId> {
        let mut chunk_ids = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Ok(file_name) = entry.file_name().into_string() {
                        if file_name.len() == 64 {
                            if let Ok(chunk_id) = hex::decode(file_name) {
                                if chunk_id.len() == 32 {
                                    let mut chunk_id_array = [0u8; 32];
                                    chunk_id_array.copy_from_slice(&chunk_id);
                                    chunk_ids.push(chunk_id_array);
                                }
                            }
                        }
                    }
                }
            }
        }

        chunk_ids
    }
}

#[cfg(test)]
mod tests {
    use crate::DataManagerImpl;
    use super::*;

    #[test]
    fn test_instantiate_local_data_source() {
        let ds = LocalDataSource::new(PathBuf::from("./test_data_dir"));
        assert_eq!(ds.data_dir, PathBuf::from("./test_data_dir"));
    }

    #[test]
    fn test_list_files_as_chunk_ids() {
        let ds = LocalDataSource::new(PathBuf::from("./test_data_dir"));
        let chunk_ids = ds.list_files_as_chunk_ids();
        assert_eq!(chunk_ids.len(), 4);
        assert_eq!(chunk_ids[0], [0u8; 32]);
        assert_eq!(chunk_ids[1], [17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17]);
        assert_eq!(chunk_ids[2], [123, 94, 164, 195, 214, 231, 248, 169, 176, 193, 210, 227, 244, 165, 182, 199, 216, 233, 240, 161, 178, 195, 212, 229, 246, 167, 184, 201, 208, 225, 242, 163]);
        assert_eq!(chunk_ids[3], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
    }


}