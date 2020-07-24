use std::sync::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::error::Error;


#[derive(Clone)]
pub struct WorkSender<T> {
    sender: Sender<T>,
    length: Arc<AtomicUsize>,
}

impl<T: 'static + Send> WorkSender<T> {
    pub fn send(&self, message: T) -> Result<(), Box<dyn Error>> {
        self.sender.send(message)?;
        self.length.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.length.load(Ordering::Relaxed)
    }
}

pub struct WorkReceiver<T> {
    receiver: Receiver<T>,
    length: Arc<AtomicUsize>,
}

impl<T: Send> WorkReceiver<T> {
    pub fn try_recv(&self) -> Result<T, Box<dyn Error>> {
        let message = self.receiver.try_recv()?;
        self.length.fetch_sub(1, Ordering::Relaxed);
        Ok(message)
    }

    pub fn len(&self) -> usize {
        self.length.load(Ordering::Relaxed)
    }
}

pub fn work_channel<T>() -> (WorkSender<T>, WorkReceiver<T>) {
    let (sender, receiver) = channel();
    let length = Arc::new(AtomicUsize::new(0));
    let work_sender = WorkSender {
        sender,
        length: length.clone(),
    };
    let work_receiver = WorkReceiver {
        receiver,
        length,
    };
    (work_sender, work_receiver)
}
