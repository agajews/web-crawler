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
use std::thread::sleep;

pub struct DiskPQueueReceiver {
    config: Config,
    work_receiver: Receiver<Option<Job>>,
    event_sender: Sender<PQueueEvent>,
    n_requests: usize,
    monitor: MonitorHandle,
}

impl DiskPQueueReceiver {
    pub fn pop(&mut self) -> Option<Job> {
        let maybe_job = self.work_receiver.try_recv().ok();
        if maybe_job.is_some() {
            self.n_requests -= 1;
            if maybe_job.as_ref().unwrap().is_none() {
                self.monitor.inc_missing_job();
            }
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
    read_map: BTreeMap<usize, Vec<PageEvent>>,
}

enum PageEvent {
    Inc(Job),
}

impl DiskPQueue {
    pub fn spawn(id: usize, config: Config, monitor: MonitorHandle) -> (DiskPQueueSender, DiskPQueueReceiver) {
        let (work_sender, work_receiver) = channel();
        let (event_sender, event_receiver) = channel();
        let mut thread_event_senders = Vec::new();
        for tid in 0..config.n_pqueue_threads {
            let event_sender = event_sender.clone();
            let thread_event_sender = DiskPQueueThread::spawn(
                id,
                tid,
                config.clone(),
                event_sender,
            );
            thread_event_senders.push(thread_event_sender);
        }
        let mut cache = BTreeCache::new();
        cache.insert(0, Page::init(&config));
        let mut page_table = BTreeMap::new();
        page_table.insert(PageBoundsCmp::init(), 0);
        let mut pqueue = PriorityQueue::new();
        pqueue.push(0, 0);
        let mut pqueue = DiskPQueue {
            work_sender,
            thread_event_senders,
            cache,
            monitor: monitor.clone(),
            config: config.clone(),
            page_table: page_table,
            pqueue: pqueue,
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
            monitor,
        };
        (sender, receiver)
    }

    fn cache_page(event_senders: &[Sender<DiskThreadEvent>], config: &Config, cache: &mut BTreeCache<usize, Page>, id: usize, page: Page) {
        cache.insert(id, page);
        if cache.len() > config.pqueue_cache_cap {
            let (old_id, old_page) = cache.remove_oldest().unwrap();
            Self::write_page(event_senders, old_id, old_page);
        }
    }

    fn query_cache<'a>(cache: &'a mut BTreeCache<usize, Page>, id: usize) -> Option<&'a mut Page> {
        cache.get_mut(&id)
    }

    fn run(&mut self, event_receiver: Receiver<PQueueEvent>) {
        loop {
            let event = match event_receiver.try_recv() {
                Ok(event) => event,
                Err(_) => {
                    self.monitor.inc_pqueue_free();
                    sleep(self.config.pqueue_sleep);
                    continue;
                },
            };
            match event {
                PQueueEvent::ReadResponse(id, page) => {
                    let Self { ref mut cache, ref mut thread_event_senders, ref config, .. } = *self;
                    Self::cache_page(thread_event_senders, config, cache, id, page);
                    for action in self.read_map.remove(&id).unwrap() {
                        match action {
                            PageEvent::Inc(job) => self.increment(job),
                        }
                    }
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
        let Self { ref mut page_table, ref mut pqueue, ref mut cache, ref monitor, ref mut read_map, ref mut thread_event_senders, ref config, .. } = *self;
        let (initial_bounds, &id) = page_table.get_key_value(&job.cmp_ref()).unwrap();
        let initial_bounds = (*initial_bounds).clone();
        match Self::query_cache(cache, id) {
            Some(page) => {
                let res = page.increment(job, monitor);
                pqueue.change_priority(&id, page.value).unwrap();
                // println!("page {} has priority {}", id, page.value);
                if let Some(new_page) = res {
                    // println!("splitting off into a new page");
                    // println!("left bounds: {:?}, right bounds: {:?}", page.bounds_cmp(), new_page.bounds_cmp());
                    // update old page
                    page_table.remove(&initial_bounds);
                    page_table.insert(page.bounds_cmp(), id);

                    // insert new page
                    let new_id = page_table.len();
                    page_table.insert(new_page.bounds_cmp(), new_id);
                    pqueue.push(new_id, new_page.value);
                    Self::cache_page(thread_event_senders, config, cache, new_id, new_page);
                }
            },
            None => {
                if !read_map.contains_key(&id) {
                    Self::request_page(thread_event_senders, id);
                }
                read_map.entry(id)
                    .or_insert(Vec::new())
                    .push(PageEvent::Inc(job));
            },
        }
    }

    fn pop(&mut self) -> Option<()> {
        let Self { ref mut pqueue, ref mut cache, ref work_sender, ref mut read_map, ref thread_event_senders, ref monitor, .. } = *self;
        let (&id, &priority) = match pqueue.peek() {
            Some(tup) => tup,
            None => {
                monitor.inc_missing_job();
                return None;
            },
    };
        match Self::query_cache(cache, id) {
            Some(page) => {
                match page.pop() {
                    Some(job) => {
                        self.monitor.inc_total_priority(priority);
                        // println!("popping job {}", job.url);
                        pqueue.change_priority(&id, page.value).unwrap();
                        work_sender.send(Some(job)).unwrap();
                    },
                    None => {
                        if priority > 0 {
                            panic!("failed to pop job with supposed priority {}", priority);
                        } else {
                            work_sender.send(None).unwrap();
                        }
                    }
                }
            },
            None => {
                work_sender.send(None).unwrap();
                if !read_map.contains_key(&id) {
                    Self::request_page(thread_event_senders, id);
                    read_map.insert(id, Vec::new());
                }
            }
        }
        Some(())
    }

    fn request_page(event_senders: &[Sender<DiskThreadEvent>], id: usize) {
        event_senders[id % event_senders.len()].send(DiskThreadEvent::Read(id)).unwrap();
    }

    fn write_page(event_senders: &[Sender<DiskThreadEvent>], id: usize, page: Page) {
        event_senders[id % event_senders.len()].send(DiskThreadEvent::Write(id, page)).unwrap();
    }
}
