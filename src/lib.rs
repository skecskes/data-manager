use std::path::PathBuf;
use std::thread;
use crate::data_catalogue::DataCatalogue;
use crate::data_chunk::{ChunkId, DataChunk, DataChunkRef, DatasetId};
use crate::data_manager::DataManager;
use crate::event_loop::TasksManager;
use crate::local_data_source::LocalDataSource;

pub mod data_chunk;
mod data_manager;
mod local_data_source;
mod io_operation;
mod event_loop;
mod data_catalogue;

#[derive(Debug, Clone)]
#[derive(PartialEq)]
pub enum ChunkStatus {
    Downloading,
    Ready,
    Deleting,
    Deleted,
}

pub struct DataManagerImpl {
    pub data_source: LocalDataSource,
    pub tasks_manager: TasksManager,
    pub data_catalogue: DataCatalogue,
}

impl DataManager for DataManagerImpl {
    fn new(data_dir: PathBuf) -> Self {
        let data_source = LocalDataSource::new(data_dir);
        let local_chunk_ids = data_source.read_local_chunks();

        DataManagerImpl {
            data_source,
            tasks_manager: TasksManager::default(),
            data_catalogue: DataCatalogue::new(local_chunk_ids),
        }
    }

    /// Schedule `chunk` download in background
    fn download_chunk(&self, chunk: DataChunk) {
        let task_waker = self.tasks_manager.add_future_to_manager_pool();
        if !self.data_catalogue.start_download(&chunk) {
            // don't try to download the chunk if it's already being processed
            return;
        }

        thread::spawn(move || {
            let result = LocalDataSource::download_chunk(self.data_source.data_dir.clone(), chunk.clone());
            TasksManager::wake_the_future(task_waker);
            self.data_catalogue.update_chunk(&chunk, &ChunkStatus::Ready);
            result
        });
    }

    /// List chunks, that are currently available
    fn list_chunks(&self) -> Vec<ChunkId> {
        self.data_catalogue.get_ready_chunk_ids()
    }

    fn find_chunk(&self, dataset_id: DatasetId, block_number: u64) -> Option<impl DataChunkRef> {
        self.data_catalogue.get_chunk_by_dataset_and_block(&dataset_id, block_number)
    }

    fn delete_chunk(&self, chunk_id: ChunkId) {
        let task_waker = self.tasks_manager.add_future_to_manager_pool();
        let chunk = self.data_catalogue.get_chunk_by_id(&chunk_id);
        match chunk {
            Some(chunk) => {
                if !self.data_catalogue.start_deletion(chunk) {
                    // don't try to delete the chunk if it's not ready
                    return;
                }
                thread::spawn(move || {
                    let result = LocalDataSource::delete_chunk(self.data_source.data_dir.clone(), chunk_id);
                    TasksManager::wake_the_future(task_waker);

                    self.data_catalogue.update_chunk(&chunk, &ChunkStatus::Deleted);
                    result
                });
            },
            None => {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use super::*;

    #[test]
    fn test_instantiate_data_manager() {
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 4);
        assert!(chunk_ids.contains_key(&[0u8; 32]));
    }

    #[test]
    fn test_list_chunks() {
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk_ids = data_manager.list_chunks();
        assert_eq!(chunk_ids.len(), 4);
    }

    #[test]
    fn test_download_new_chunk() {

        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk = DataChunk {
            id: [5u8; 32],
            dataset_id: [0u8; 32],
            block_range: 0..0,
            files: Default::default()
        };
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 4);
        }

        // Act
        data_manager.download_chunk(chunk.clone());

        // Assert
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert!(chunk_ids.contains_key(&[5u8; 32]));
            assert_eq!(chunk_ids.get(&[5u8; 32]), Some(&ChunkStatus::Downloading));
        }
        // wait for the download to complete before asserting anything
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(300));
        });

        // Assert
        // check if the chunk was added to the list of chunks
        let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 5);
        assert!(chunk_ids.contains_key(&[5u8; 32]));
        assert_eq!(chunk_ids.get(&[5u8; 32]), Some(&ChunkStatus::Ready));

        let chunk_id = hex::encode(chunk.id);
        let file_path = data_manager.data_source.data_dir.join(&chunk_id);
        fs::remove_file(file_path).expect("Failed to remove file");
    }

    #[test]
    fn test_download_existing_chunk() {
        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk = DataChunk {
            id: [0u8; 32],
            dataset_id: [0u8; 32],
            block_range: 0..0,
            files: Default::default()
        };
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 4);
        }

        // Act
        data_manager.download_chunk(chunk.clone());
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(300));
        });

        // Assert
        let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 4);
    }

    #[test]
    fn test_delete_existing_chunk() {
        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk = DataChunk {
            id: [0u8; 32],
            dataset_id: [0u8; 32],
            block_range: 0..0,
            files: Default::default()
        };
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 4);
        }

        // Act
        data_manager.delete_chunk(chunk.id);

        // Assert
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 4);
            assert_eq!(chunk_ids.get(&[0u8; 32]), Some(&ChunkStatus::Deleting));
        }
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(300));
        });
        let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 3);
        assert_eq!(chunk_ids.get(&[0u8; 32]), None);

        // put back the deleted chunk for next tests
        let chunk_id = hex::encode(chunk.id);
        let file_path = data_manager.data_source.data_dir.join(&chunk_id);
        fs::write(file_path, b"").expect("Failed to write file");
    }

    #[test]
    fn test_delete_non_existing_chunk() {
        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk_id = [3u8; 32];
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 4);
            assert!(!chunk_ids.contains_key(&[3u8; 32]));
        }

        // Act
        data_manager.delete_chunk(chunk_id);

        // Assert
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 4);
            assert!(!chunk_ids.contains_key(&[3u8; 32]));
        }
    }

    #[test]
    fn test_delete_not_ready_chunk() {
        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk = DataChunk {
            id: [1u8; 32],
            dataset_id: [0u8; 32],
            block_range: 0..0,
            files: Default::default()
        };
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 4);
        }

        // Act
        data_manager.download_chunk(chunk.clone());
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(50));
        });
        data_manager.delete_chunk(chunk.id);

        // Assert
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 5);
            assert_eq!(chunk_ids.get(&[1u8; 32]), Some(&ChunkStatus::Downloading));
        }
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(300));
        });
        let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 5);
        assert_eq!(chunk_ids.get(&[1u8; 32]), Some(&ChunkStatus::Ready));

        // Clean up
        let chunk_id = hex::encode(chunk.id);
        let file_path = data_manager.data_source.data_dir.join(&chunk_id);
        fs::remove_file(file_path).expect("Failed to remove file");
    }
}



