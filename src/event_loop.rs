use std::sync::{Arc, RwLock};
use futures::executor::ThreadPool;
use crate::io_operation::TaskWaker;

pub struct TasksManager {
    pool_managing_async_tasks: ThreadPool,
}

impl Default for TasksManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TasksManager {
    pub fn new() -> Self {
        TasksManager {
            pool_managing_async_tasks: ThreadPool::new().expect("Failed to create thread pool"),
        }
    }

    pub fn add_future_to_manager_pool(&self) -> Arc<RwLock<TaskWaker>> {
        let shared_waker = Arc::new(RwLock::new(TaskWaker { waker: None }));
        
        // the future
        let io_operation = crate::io_operation::IOOperation {
            task_waker: shared_waker.clone(),
        };

        // spawn the future in a thread pool
        self.pool_managing_async_tasks.spawn_ok(async {
            let result = io_operation.await;
            println!("{}", result);
        });
        shared_waker
    }
    
    /// Wake the future to allow it to finish
    pub fn wake_the_future(shared_waker: Arc<RwLock<TaskWaker>>) {
        let task_waker = shared_waker.read().unwrap();
        if let Some(waker) = &task_waker.waker {
            waker.wake_by_ref();
        }
    }
}