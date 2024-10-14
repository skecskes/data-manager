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

        let data_dir = self.data_source.data_dir.clone();
        let data_catalogue = self.data_catalogue.clone();
        thread::spawn(move || {
                let result = LocalDataSource::download_chunk(data_dir, chunk.clone());
                TasksManager::wake_the_future(task_waker);
                data_catalogue.update_chunk(&chunk, &ChunkStatus::Ready);
                result
            }
        );
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
                if !self.data_catalogue.start_deletion(&chunk) {
                    // don't try to delete the chunk if it's not ready
                    return;
                }
                thread::spawn({
                    let data_dir = self.data_source.data_dir.clone();
                    let chunk = chunk.clone();
                    let task_waker = task_waker.clone();
                    let data_catalogue = self.data_catalogue.clone();

                    move || {
                        let result = LocalDataSource::delete_chunk(data_dir, chunk_id);
                        TasksManager::wake_the_future(task_waker);

                        data_catalogue.update_chunk(&chunk, &ChunkStatus::Deleted);
                        result
                    }

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
    use serial_test::serial;
    use crate::local_data_source::{get_test_chunk_111111_0_35, get_test_chunk_111111_107_135, get_test_chunk_111111_36_94, get_test_chunk_111111_95_106};
    use super::*;

    #[test]
    #[serial]
    fn test_instantiate_data_manager() {
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 8);
        assert!(chunk_ids.contains_key(&[147, 161, 202, 94, 141, 129, 235, 161, 211, 123, 214, 159, 212, 119, 7, 59, 107, 144, 48, 224, 108, 245, 142, 139, 2, 173, 240, 231, 54, 58, 115, 159]));
        assert!(chunk_ids.contains_key(&[52, 249, 87, 41, 193, 143, 108, 194, 169, 137, 151, 250, 99, 44, 49, 211, 165, 208, 160, 65, 58, 31, 238, 223, 208, 29, 143, 142, 93, 6, 220, 211]));
        assert!(chunk_ids.contains_key(&[193, 57, 118, 234, 133, 186, 129, 98, 68, 9, 137, 174, 130, 138, 250, 203, 200, 19, 226, 101, 224, 108, 235, 80, 186, 6, 49, 14, 23, 58, 108, 70]));
        assert!(chunk_ids.contains_key(&[118, 166, 206, 104, 188, 72, 255, 213, 176, 59, 193, 246, 55, 235, 118, 138, 87, 148, 244, 77, 58, 207, 103, 229, 97, 58, 212, 176, 79, 143, 187, 4]));
        assert!(chunk_ids.contains_key(&[98, 39, 185, 12, 229, 13, 3, 121, 220, 39, 48, 2, 38, 129, 54, 147, 17, 92, 89, 191, 47, 125, 227, 35, 162, 83, 99, 140, 124, 47, 92, 153]));
        assert!(chunk_ids.contains_key(&[47, 214, 124, 127, 237, 100, 240, 96, 40, 147, 96, 68, 104, 154, 218, 127, 165, 181, 128, 44, 47, 16, 60, 172, 24, 208, 88, 136, 149, 79, 243, 191]));
        assert!(chunk_ids.contains_key(&[56, 24, 248, 27, 82, 241, 162, 191, 1, 219, 253, 77, 160, 250, 121, 88, 143, 116, 109, 77, 123, 216, 197, 83, 201, 51, 240, 120, 186, 231, 249, 76]));
        assert!(chunk_ids.contains_key(&[168, 77, 161, 67, 100, 46, 30, 66, 3, 236, 122, 88, 18, 185, 131, 120, 153, 130, 152, 113, 236, 29, 91, 3, 244, 6, 254, 177, 61, 66, 182, 178]));

    }

    #[test]
    #[serial]
    fn test_list_chunks() {
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk_ids = data_manager.list_chunks();
        assert_eq!(chunk_ids.len(), 8);
    }

    #[test]
    #[serial]
    fn test_download_new_chunk() {
        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk = get_test_chunk_111111_95_106();

        // Assert initial state
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 8);
            assert!(!chunk_ids.contains_key(&chunk.id));
        }

        // Act
        data_manager.download_chunk(chunk.clone());

        // Assert transitional state
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(15));
        });
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 9);
            assert_eq!(chunk_ids.get(&chunk.id), Some(&ChunkStatus::Downloading));
        }

        // Asserting final state
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(200));
        });
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            // chunk was added to the list of chunks and is in `Ready` state
            assert_eq!(chunk_ids.len(), 9);
            assert!(chunk_ids.contains_key(&chunk.id));
            assert_eq!(chunk_ids.get(&chunk.id), Some(&ChunkStatus::Ready));
        }
        // cleanup
        data_manager.delete_chunk(chunk.id);
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(50));
        });
    }

    #[test]
    #[serial]
    fn test_download_existing_chunk() {
        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk = get_test_chunk_111111_0_35();
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 8);
        }

        // Act
        data_manager.download_chunk(chunk.clone());
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(200));
        });

        // Assert
        let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 8);
    }

    #[test]
    #[serial]
    fn test_delete_existing_chunk() {
        // Arrange
        let data_dir = PathBuf::from("./local_data_dir");
        let data_manager = DataManagerImpl::new(data_dir);
        let chunk = get_test_chunk_111111_107_135();
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 8);
        }
        data_manager.download_chunk(chunk.clone());
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 9);
        }
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(200));
        });

        // Act
        data_manager.delete_chunk(chunk.id);

        // Assert deleting
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 9);
            assert_eq!(chunk_ids.get(&chunk.id), Some(&ChunkStatus::Deleting));
        }
        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(200));
        });

        // Assert deleted
        let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 8);
        assert_eq!(chunk_ids.get(&chunk.id), None);
    }

    #[test]
    #[serial]
    fn test_delete_non_existing_chunk() {
        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk_id = [3u8; 32];
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 8);
            assert!(!chunk_ids.contains_key(&[3u8; 32]));
        }

        // Act
        data_manager.delete_chunk(chunk_id);

        // Assert
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 8);
            assert!(!chunk_ids.contains_key(&[3u8; 32]));
        }
    }

    #[test]
    #[serial]
    fn test_delete_not_ready_chunk() {
        // Arrange
        let data_manager = DataManagerImpl::new(PathBuf::from("./local_data_dir"));
        let chunk = get_test_chunk_111111_0_35();
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 8);

        }
        // chunk that is in downloading or deleting state can't be deleted again.
        data_manager.data_catalogue.update_chunk(&chunk, &ChunkStatus::Deleting);


        // Act
        data_manager.delete_chunk(chunk.id);

        // Assert
        {
            let chunk_ids = data_manager.data_catalogue.chunk_ids.lock().unwrap();
            assert_eq!(chunk_ids.len(), 8);
            assert_eq!(chunk_ids.get(&chunk.id), Some(&ChunkStatus::Deleting));
        }
    }
}



