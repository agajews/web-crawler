pub struct DiskPQueueReceiver {
    config: Config,
    work_receiver: Receiver<Job>,
    event_sender: Sender<PQueueEvent>,
    n_requests: usize,
}

impl DiskPQueueReceiver {
    pub fn pop(&mut self) -> Option<Job> {
        let maybe_job = self.work_receiver.try_recv().ok();
        if maybe_job.is_some() {
            self.n_requests -= 1;
        }
        while self.n_requests < self.config.scheduler_queue_cap {
            self.event_sender.send(PQueueEvent::Pop).unwrap();
            self.n_requests += 1;
        }
        maybe_job
    }
}

#[derive(Clone)]
pub struct DiskPQueueSender {
    event_sender: Sender<PQueueEvent>,
}

impl DiskPQueueSender {
    pub fn increment(&self, job: Job) {
        self.event_sender.send(PQueueEvent::Inc(job)).unwrap();
    }
}

pub struct DiskPQueue {
    config: Config,
    work_sender: Sender<Job>,
    event_receivers: Receiver<PQueueEvent>,
    thread_event_senders: Vec<Sender<DiskThreadEvent>>,
    page_table: BTreeMap<PageBoundsCmp, usize>,
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
        let (event_sender, event_receiver) = channel();
        let mut thread_event_senders = Vec::new();
        for _ in 0..config.n_pqueue_threads {
            let event_sender = event_sender.clone();
            page_writers.push(page_writer);
            let thread_event_sender = DiskPQueueThread::spawn(
                page_request_receiver,
                page_write_request_receiver,
                event_sender,
            );
            thread_event_senders.push(thread_event_sender);
        }
        let mut cache = BTreeCache::new();
        cache.push(0, Page::init(&config));
        let pqueue = DiskPQueue {
            config,
            work_sender,
            event_receiver,
            thread_event_senders,
            cache,
            page_table: BTreeMap::new(),
            pqueue: PriorityQueueMap::new(),
        };
        thread::spawn(move || pqueue.run());
        let sender = DiskPQueueSender {
            event_sender: event_sender.clone(),
        };
        let receiver = DiskPQueueReceiver {
            work_receiver,
            event_sender,
            outstanding: 0,
            config: config.clone(),
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
        for event in self.event_receiver {
            match event {
                PQueueEvent::ReadResponse(id, page) => {
                    self.cache_page(id, page);
                    for action in self.read_map.remove(id).unwrap() {
                        match action {
                            PageEvent::Inc(job) => self.increment(job),
                            _ => (),
                        }
                    }
                },
                PQueueEvent::WriteResponse(id) => {
                    self.write_map.remove(page_id);
                },
                PQueueEvent::IncRequest(job) => {
                    self.increment(job);
                },
                PQueueEvent::PopRequest => {
                    self.pop();
                },
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
                    self.page_table.insert(page.bounds, id);

                    // insert new page
                    let new_id = self.page_table.len();
                    self.page_table.insert(new_page.bounds, new_id);
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
