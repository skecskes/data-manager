# Data Manager

Manages data chunks availability for the workers. This is the only interface that
should be exposed to the workers.

- spawn future tasks for downloading and deleting data chunks using Tasks Manager
- lists available data chunks and finds a chunk responsible for a given block number are provided blocking methods
- data chunks are stored in the `local_data_dir` directory
- orchestrates background tasks, data sources and data catalogue

# Tasks Manager

Manages tasks that needs to be done in background with just 2 methods:

- owns thread pool of tasks(IO Operations)
- method to spawn a future task in thread pool for background tasks
- static method for waking up the task threads
- possible to extend with mpsc channel for communication between tasks

# IO Operation

An async task for background operations

- implements polling from Futures for IO operations
- basically this is async wrapper for tasks that needs to be done in background

Image, because it's better describing than words:
![Tasks Manager](task%20manager.png)


# Data Catalogue

Manages in memory registry of data chunks and their availability.

- loads stored data chunks from the `local_data_dir` directory
- provides methods to list available data chunks and find a chunk responsible for a given block number
- uses RwLock for data chunks registry to prevent multiple threads from accessing the data at the same time

# Local Data Source

Implements data source for local file system

- provides methods to download and delete data chunks
- can read available data chunks from the `local_data_dir` directory

Nice to have: We might implement common trait and various implementations for different data sources like local file system, S3, etc.

# Code examples'

```rust
        use std::collections::HashMap;
        use data_manager::data_manager::DataManagerImpl;
        use data_manager::data_chunk::DataChunk;

        // instantiate data manager
        let data_manager = DataManagerImpl::default();

        // download a chunk
        let chunk = DataChunk {
            id: [1u8; 32],
            dataset_id: [1u8; 32],
            block_range: 0..35,
            files: HashMap::from([
                ("file.parquet".to_string(), "https://example.com/111111/0/35/file1.parquet".to_string())
            ]),
        };
        data_manager.download_chunk(chunk);
        
        // find a specific chunk
        let the_chunk = data_manager.find_chunk([1u8; 32], 0);
        
        // delete available chunks
        data_manager.delete_chunk([1u8; 32]);

        // list available chunks
        let chunks = data_manager.list_chunks();
```
