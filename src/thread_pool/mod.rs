use std::thread;
use std::sync::{mpsc::{Receiver, Sender, channel}, Arc, Mutex};

use log::trace;


type Message = Box<dyn FnOnce() + Send + 'static>;
pub struct ThreadPool {
    worker: Vec<Worker>,
    sender: Option<Sender<Message>>,
    receiver: Option<Arc<Mutex<Receiver<Message>>>>,
}

pub struct Worker {
    handle: thread::JoinHandle<()>,
    id: usize,
}

/// use polling to detect panicked threads
///
/// drop for automatic cleaning

impl ThreadPool {
    pub fn new(n: usize) -> Self {
        let (tx, rx) = channel::<Message>();
        let mut worker = Vec::new();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..n {
            worker.push(Worker::new(i, Arc::clone(&rx)));
        }

        Self {
            worker,
            sender: Some(tx),
            receiver: Some(rx),
        }
    }

    pub fn spawn(&self, task: Message) {
        self.sender.as_ref().unwrap().send(task).unwrap();
    }

    pub fn poll(&mut self) {
        let dead: Vec<usize> = self.worker.iter()
                                            .filter(|&x| x.is_end())
                                            .map(|x| x.id)
                                            .collect();
        for &i in dead.iter() {
            self.worker[i] = Worker::new(i, Arc::clone(self.receiver.as_ref().unwrap()));
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.receiver.take());
        drop(self.sender.take());

        for worker in self.worker.drain(..) {
            trace!("Error in joining thread {}", worker.id);
            worker.handle.join().expect("Error happens in joining thread");
        }
    }
}

impl Worker {
    pub fn new(id: usize, rx: Arc<Mutex<Receiver<Message>>>) -> Self {
        let handle = thread::spawn(move || {
            loop {
                let message = rx.lock()
                                                                        .unwrap()
                                                                        .recv();
                match message {
                    Ok(f) => {
                        trace!("thread {} receives a task.", id);
                        f();
                    },
                    Err(_) => {
                        trace!("thread {} shuts down", id);
                        break;
                    }
                }
            }
        });

        Self {
            handle,
            id,
        }
    }

    fn is_end(&self) -> bool {
        self.handle.is_finished()
    }
}