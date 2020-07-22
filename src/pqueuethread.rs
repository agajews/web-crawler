
DiskPQueueThread::spawn(
    page_request_receiver,
    page_sender,
    page_write_request_receiver,
    page_write_sender,
);

struct DiskPQueueThread {
    page_request_receiver: Receiver<usize>,
    page_sender: Sender<(usize, Page)>,
    page_write_request_receiver: Receiver<(usize, Page)>,
    page_write_sender: Sender<usize>,
}

impl DiskPQueueThread {
    fn spawn(
        page_request_receiver: Receiver<usize>,
        page_sender: Sender<(usize, Page)>,
        page_write_request_receiver: Receiver<(usize, Page)>,
        page_write_sender: Sender<usize>,
    ) {
        let pqueue_thread = DiskPQueueThread {
            page_request_receiver,
            page_sender,
            page_write_request_receiver,
            page_write_sender,
        };
        thread::spawn(move || pqueue_thread.run());
    }

    fn run(&self) {
    }
}
