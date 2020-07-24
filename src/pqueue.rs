use crate::pqueueevent::PQueueEvent;
use crate::config::Config;
use crate::monitor::MonitorHandle;
use crate::page::Page;
use crate::pqueuethread::{DiskPQueueThread, DiskThreadEvent};
use crate::job::Job;
use crate::pagebounds::PageBoundsCmp;
use crate::btreecache::BTreeCache;

use std::sync::mpsc::{channel, Receiver, Sender};
use priority_queue::PriorityQueue;
use std::collections::BTreeMap;
use std::thread;

pub struct DiskPQueueReceiver {
    config: Config,
    work_receiver: Receiver<Option<Job>>,
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
            self.event_sender.send(PQueueEvent::PopRequest).unwrap();
            self.n_requests += 1;
        }
        maybe_job.flatten()
    }
}

#[derive(Clone)]
pub struct DiskPQueueSender {
    event_sender: Sender<PQueueEvent>,
}

impl DiskPQueueSender {
    pub fn increment(&self, job: Job) {
        self.event_sender.send(PQueueEvent::IncRequest(job)).unwrap();
    }
}

pub struct DiskPQueue {
    config: Config,
    monitor: MonitorHandle,
    work_sender: Sender<Option<Job>>,
    thread_event_senders: Vec<Sender<DiskThreadEvent>>,
    page_table: BTreeMap<PageBoundsCmp, usize>,
    pqueue: PriorityQueue<usize, u32>,
    cache: BTreeCache<usize, Page>,
    write_map: BTreeMap<usize, Page>,
    read_map: BTreeMap<usize, Vec<PageEvent>>,
}

enum PageEvent {
    Inc(Job),
    Pop,
}

impl DiskPQueue {
    pub fn spawn(config: Config, monitor: MonitorHandle) -> (DiskPQueueSender, DiskPQueueReceiver) {
        let (work_sender, work_receiver) = channel();
        let (event_sender, event_receiver) = channel();
        let mut thread_event_senders = Vec::new();
        for tid in 0..config.n_pqueue_threads {
            let event_sender = event_sender.clone();
            let thread_event_sender = DiskPQueueThread::spawn(
                tid,
                config.clone(),
                event_sender,
            );
            thread_event_senders.push(thread_event_sender);
        }
        let mut cache = BTreeCache::new();
        cache.insert(0, Page::init(&config));
        let mut pqueue = DiskPQueue {
            monitor,
            work_sender,
            thread_event_senders,
            cache,
            config: config.clone(),
            page_table: BTreeMap::new(),
            pqueue: PriorityQueue::new(),
            write_map: BTreeMap::new(),
            read_map: BTreeMap::new(),
        };
        thread::spawn(move || pqueue.run(event_receiver));
        let sender = DiskPQueueSender {
            event_sender: event_sender.clone(),
        };
        let receiver = DiskPQueueReceiver {
            config,
            work_receiver,
            event_sender,
            n_requests: 0,
        };
        (sender, receiver)
    }

    fn cache_page(&mut self, id: usize, page: Page) {
        self.cache.insert(id, page);
        if self.cache.len() > self.config.pqueue_cache_cap {
            let (old_id, old_page) = self.cache.remove_oldest().unwrap();
            self.write_map.insert(old_id, old_page.clone());
            self.write_page(old_id, old_page);
        }
    }

    fn query_cache(&mut self, id: usize) -> Option<&mut Page> {
        if self.write_map.contains_key(&id) {
            return self.write_map.get_mut(&id);
        } else {
            return self.cache.get_mut(&id);
        }
    }

    fn run(&mut self, event_receiver: Receiver<PQueueEvent>) {
        for event in event_receiver {
            match event {
                PQueueEvent::ReadResponse(id, page) => {
                    self.cache_page(id, page);
                    for action in self.read_map.remove(&id).unwrap() {
                        match action {
                            PageEvent::Inc(job) => self.increment(job),
                            _ => (),
                        }
                    }
                },
                PQueueEvent::WriteResponse(id) => {
                    self.write_map.remove(&id);
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
        let (initial_bounds, id) = (initial_bounds.clone(), id.clone());
        match self.query_cache(id) {
            Some(page) => {
                if let Some(new_page) = page.increment(job, &self.monitor) {
                    // update old page
                    self.page_table.remove(initial_bounds);
                    self.page_table.insert(page.bounds_cmp(), id);

                    // insert new page
                    let new_id = self.page_table.len();
                    self.page_table.insert(new_page.bounds_cmp(), new_id);
                    self.cache_page(new_id, new_page);
                    self.pqueue.push(new_id, new_page.value);
                }
                self.pqueue.change_priority(&id, page.value);
            },
            None => {
                if !self.read_map.contains_key(&id) {
                    self.request_page(id);
                }
                self.read_map.entry(id)
                    .or_insert(Vec::new())
                    .push(PageEvent::Inc(job));
            },
        }
    }

    fn pop(&mut self) -> Option<()> {
        let (id, _) = self.pqueue.peek()?;
        match self.query_cache(*id) {
            Some(page) => {
                match page.pop() {
                    Some(job) => {
                        self.pqueue.change_priority(id, page.value);
                        self.work_sender.send(Some(job)).unwrap();
                    },
                    None => {
                        self.work_sender.send(None).unwrap();
                    }
                }
            },
            None => {
                self.work_sender.send(None).unwrap();
                if !self.read_map.contains_key(id) {
                    self.request_page(*id);
                    self.read_map.insert(*id, Vec::new());
                }
            }
        }
        Some(())
    }

    fn request_page(&mut self, id: usize) {
        self.thread_event_senders[id % self.thread_event_senders.len()].send(DiskThreadEvent::Read(id)).unwrap();
    }

    fn write_page(&mut self, id: usize, page: Page) {
        self.thread_event_senders[id % self.thread_event_senders.len()].send(DiskThreadEvent::Write(id, page)).unwrap();
    }
}
