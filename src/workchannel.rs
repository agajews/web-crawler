use std::sync::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::mpsc::{Sender, Receiver};


#[derive(Clone)]
struct<T> WorkSender<T> {
    sender: Sender<T>,
    len: Arc<AtomicUsize>,
}

impl<T> WorkSender<T> {
    pub fn send(&self, message: T) -> Result<(), Box<dyn Error>> {
        self.sender.send(message)?;
        self.len.fetch_add(1, Ordering::Relaxed);
    }
}

struct<T> WorkReceiver<T> {
    receiver: Receiver<T>,
    len: Arc<AtomicUsize>,
}

impl<T> WorkReceiver<T> {
    pub fn try_recv(&self) -> Result<T, Box<dyn Error>> {
        let message = self.receiver.try_recv()?;
        self.len.fetch_sub(1, Ordering::Relaxed);
        Ok(message)
    }
}

fn<T> work_channel() -> (WorkSender<T>, WorkReceiver<T>) {
    let (sender, receiver) = channel();
    let len = Arc::new(AtomicUsize::new(0));
    let work_sender = WorkSender {
        sender,
        len: len.clone(),
    }
    let work_receiver = WorkReceiver {
        receiver,
        len,
    }
    (work_sender, work_receiver)
}
