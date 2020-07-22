pub struct DiskPQueueReceiver {
    work_receiver: WorkReceiver<Job>,
}

#[derive(Clone)]
pub struct DiskPQueueSender {
    inc_sender: Sender<Job>,
}

pub struct DiskPQueue {
    config: Config,
    work_sender: WorkSender<Job>,
    inc_receiver: Receiver<Job>,
    page_receiver: Receiver<(PageEvent, Page)>,
    page_senders: Vec<Sender<(PageEvent, Page)>,
    page_requesters: Vec<Sender<(PageEvent, PageId)>>,
}

enum PageEvent {
    Inc(Job),
    Pop,
}

impl DiskPQueue {
    pub fn spawn(config: Config) -> (DiskPQueueSender, DiskPQueueReceiver) {
        let (work_sender, work_receiver) = work_channel();
        let (inc_sender, inc_receiver) = channel();
        let pqueue = DiskPQueue {
            config,
            work_sender,
            inc_receiver,
        };
        thread::spawn(move || pqueue.run());
        let sender = DiskPQueueSender {
            inc_sender,
        };
        let receiver = DiskPQueueReceiver {
            work_receiver,
        };
        (sender, receiver)
    }

    fn run(&mut self) {
        loop {
            if let Some((id, page)) = self.page_receiver.try_recv() {
                self.cache.insert(id, page);
                for action in self.read_map.remove(id).unwrap() {
                    match action {
                        PageEvent::Inc(job) => self.increment(job),
                        PageEvent::Pop => self.pop(),
                    }
                }
            }

            if let Some(page_id) = self.page_write_receiver.try_recv() {
                self.write_map.remove(page_id);
            }

            if let Some(job) = self.inc_receiver.try_recv() {
                self.increment(job);
            }

            if self.work_sender.len() < self.config.work_sender_cap {
                self.pop();
            }
        }
    }

    fn increment(&mut self, job: Job) {
        let id = self.page_table.find(&job);
        match self.cache.get(id) {
            Some(page) => {
                if let Some((new_page, updated_bounds)) = page.increment(job) {
                    let id = self.page_table.insert(new_page);
                    self.page_table.update(id, updated_bounds);
                    self.cache.insert(id, new_page);
                    self.pqueue.insert(id, new_page.value());
                }
                self.pqueue.update(id, page.value());
            },
            None => {
                self.read_map.add(id, PageEvent::Inc(job));
                self.page_requester.send(id).unwrap();
            },
        }
    }

    fn pop(&mut self) {
        let id = self.pqueue.peek();
        match self.cache.get(id) {
            Some(page) => {
                let job = page.pop();
                self.pqueue.update(id, page.value());
                self.work_sender.send(job).unwrap();
            },
            None => {
                self.read_map.add(id, PageEvent::Pop);
                self.page_requester.send(id).unwrap();
            }
        }
    }
}
