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
    page_requesters: Vec<Sender<usize>>,
    page_write_receiver: Receiver<usize>,
    page_writers: Vec<Sender<(usize, Page)>>,
    page_table: BTreeMap<PageBounds, usize>,
    pqueue: PriorityQueueMap<usize, u32>,
    cache: BTreeCache<usize, Page>,
}

enum PageEvent {
    Inc(Job),
    Pop,
}

impl DiskPQueue {
    pub fn spawn(config: Config) -> (DiskPQueueSender, DiskPQueueReceiver) {
        let (work_sender, work_receiver) = work_channel();
        let (inc_sender, inc_receiver) = channel();
        let mut page_requesters = Vec::new();
        let mut page_writers = Vec::new();
        let (page_sender, page_receiver) = channel();
        let (page_write_sender, page_write_receiver) = channel();
        for _ in 0..config.n_pqueue_threads {
            let page_sender = page_sender.clone();
            let (page_requester, page_request_receiver) = channel();
            let (page_writer, page_write_request_receiver) = channel();
            page_requesters.push(page_requester);
            page_writers.push(page_writer);
            DiskPQueueThread::spawn(
                page_request_receiver,
                page_sender,
                page_write_request_receiver,
                page_write_sender,
            );
        }
        let pqueue = DiskPQueue {
            config,
            work_sender,
            inc_receiver,
            page_receiver,
            page_requesters,
            page_write_receiver,
            page_writers,
            page_table: BTreeMap::new(),
            pqueue: PriorityQueueMap::new(),
            cache: BTreeCache::new(),
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

    fn cache_page(&mut self, id: usize, page: Page) {
        self.cache.insert(id, page);
        if self.cache.len() > self.config.pqueue_cache_cap {
            let (old_id, old_page) = self.cache.remove_oldest().unwrap();
            self.write_map.insert(old_id, old_page);
            self.write_page(old_id, old_page);
        }
    }

    fn query_cache(&mut self, id: usize) -> Option<&Page> {
        if let Some(page) = self.cache.get_mut(id) {
            return page;
        }
        if let Some(page) = self.write_map.get_mut(id) {
            return page;
        }
        return None;
    }

    fn run(&mut self) {
        // TODO: track empty loop runs
        loop {
            if let Some((id, page)) = self.page_receiver.try_recv() {
                self.cache_page(id, page);
                for action in self.read_map.remove(id).unwrap() {
                    match action {
                        PageEvent::Inc(job) => self.increment(job),
                        _ => (),
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
        let (initial_bounds, id) = self.page_table.get_key_value(&job.cmp_ref()).unwrap();
        match self.query_cache(id) {
            Some(page) => {
                if let Some(new_page) = page.increment(job) {
                    // update old page
                    self.page_table.remove(initial_bounds);
                    self.page_table.insert(page.bounds(), id);

                    // insert new page
                    let new_id = self.page_table.len();
                    self.page_table.insert(new_page.bounds(), new_id);
                    self.cache_page(new_id, new_page);
                    self.pqueue.insert(new_id, new_page.value());
                }
                self.pqueue.update(id, page.value());
            },
            None => {
                self.read_map.add(id, PageEvent::Inc(job));
                self.request_page(id);
            },
        }
    }

    fn pop(&mut self) {
        let id = self.pqueue.peek();
        match self.query_cache(id) {
            Some(page) => {
                let job = page.pop();
                self.pqueue.update(id, page.value());
                self.work_sender.send(job).unwrap();
            },
            None => {
                if !self.page_map.contains(id) {
                    self.page_map.add(id, PageEvent::Pop);
                    self.request_page(id);
                }
            }
        }
    }

    fn request_page(&mut self, id: usize) {
        self.page_requesters[id % self.page_requesters.len()].send(id).unwrap();
    }

    fn write_page(&mut self, id: usize, page: Page) {
        self.page_writers[id % self.page_writers.len()].send((id, page)).unwrap();
    }
}
