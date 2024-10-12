use std::future::Future;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, Waker};

// Shared state between the IO operation and the event loop that will wake it
pub struct TaskWaker {
    pub waker: Option<Waker>,
}

// An asynchronous I/O operation that waits for some external event to complete
pub struct IOOperation {
    pub task_waker: Arc<RwLock<TaskWaker>>,
}

impl Future for IOOperation {
    type Output = String;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut task_waker = self.task_waker.write().unwrap();

        if task_waker.waker.is_none() {
            // Store the waker so the other thread can wake it later
            task_waker.waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            // Once woken by the separate thread, return ready
            Poll::Ready("I/O Operation completed!".to_string())
        }
    }
}