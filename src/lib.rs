use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::data_chunk::{ChunkId, DataChunk, DatasetId};
use crate::data_manager::DataManager;
use crate::event_loop::TasksManager;
use crate::local_data_source::LocalDataSource;

pub mod data_chunk;
mod data_manager;
mod local_data_source;
mod iooperation;
mod event_loop;

pub struct DataManagerImpl {
    pub data_source: LocalDataSource,
    pub chunk_ids: Arc<Mutex<Vec<ChunkId>>>,
    pub tasks_manager: TasksManager,
}

impl DataManager for DataManagerImpl {
    fn new(data_dir: PathBuf) -> Self {
        let data_source = LocalDataSource::new(data_dir);
        DataManagerImpl {
            chunk_ids: Arc::new(Mutex::new(data_source.list_existing_chunk_ids())),
            data_source,
            tasks_manager: TasksManager::default(),
        }
    }

    /// Schedule `chunk` download in background
    fn download_chunk(&self, chunk: DataChunk) {
        let state = self.tasks_manager.add_future_to_manager_pool();
        let data_dir = self.data_source.data_dir.clone();
        let chunk_ids: Arc<Mutex<Vec<ChunkId>>> = Arc::clone(&self.chunk_ids);
        let worker_thread = thread::spawn(move || {
            let mut chunk_ids = chunk_ids.lock().unwrap();
            let result = LocalDataSource::download_chunk(data_dir, chunk.clone());
            TasksManager::wake_the_future(state);
            chunk_ids.push(chunk.id);
            result
        });
        
        // if we would need to do something with result, we could join the worker handle, 
        // but that would be blocking. Rather, we could use a channel to communicate the result.
        // let result = worker_thread.join().expect("Failed to join worker thread"); 
        // println!("Result: {:?}", result);
    }

    fn list_chunks(&self) -> Vec<ChunkId> {
        self.chunk_ids.lock().unwrap().clone()
    }

    fn find_chunk(&self, dataset_id: DatasetId, block_number: u64) -> Option<DataChunk> {
        unimplemented!()
    }

    fn delete_chunk(&self, chunk_id: ChunkId) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use super::*;

    #[test]
    fn test_instantiate_data_manager() {
        let dm = DataManagerImpl::new(PathBuf::from("./test_data_dir"));
        let chunk_ids = dm.chunk_ids.lock().unwrap();
        assert_eq!(chunk_ids.len(), 4);
        assert_eq!(chunk_ids[0], [0u8; 32]);
    }

    #[test]
    fn test_list_chunks() {
        let dm = DataManagerImpl::new(PathBuf::from("./test_data_dir"));
        let chunk_ids = dm.list_chunks();
        assert_eq!(chunk_ids.len(), 4);
    }

    #[test]
    fn test_download_new_chunk() {
        let dm = DataManagerImpl::new(PathBuf::from("./test_data_dir"));
        let chunk = DataChunk {
            id: [5u8; 32],
            dataset_id: [0u8; 32],
            block_range: 0..0,
            files: Default::default()
        };
        dm.download_chunk(chunk.clone());

        futures::executor::block_on(async {
            thread::sleep(std::time::Duration::from_millis(300));
        });
        
        let chunk_id = hex::encode(chunk.id);
        let file_path = dm.data_source.data_dir.join(&chunk_id);
        fs::remove_file(file_path).expect("Failed to remove file");
    }
}



