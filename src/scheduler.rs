use crate::workchannel::WorkSender;
use crate::pqueue::DiskPQueueReceiver;
use crate::job::{Job, JobLocality};
use crate::config::Config;
use crate::monitor::MonitorHandle;

use std::sync::mpsc::{Receiver, Sender, channel};
use std::collections::BTreeMap;
use std::thread;
use std::thread::sleep;
use std::thread::JoinHandle;
use rand::Rng;
use rand::thread_rng;

pub struct Scheduler {
    monitor: MonitorHandle,
    empty_receiver: Receiver<usize>,
    register_receiver: Receiver<(WorkSender<Job>, Sender<usize>)>,
    work_senders: Vec<WorkSender<Job>>,
    pqueues: Vec<DiskPQueueReceiver>,
    config: Config,
    tid_localities: Vec<Vec<JobLocality>>,
    recent_domains: BTreeMap<JobLocality, Vec<usize>>,
    stashed_job: Option<Job>,
    pqueue_id: usize,
}

#[derive(Clone)]
pub struct SchedulerHandle {
    empty_sender: Sender<usize>,
    register_sender: Sender<(WorkSender<Job>, Sender<usize>)>,
}

impl SchedulerHandle {
    pub fn register(&self, work_sender: WorkSender<Job>) -> usize {
        let (tid_sender, tid_receiver) = channel();
        self.register_sender.send((work_sender, tid_sender)).unwrap();
        tid_receiver.recv().unwrap()
    }

    pub fn mark_empty(&self, tid: usize) {
        self.empty_sender.send(tid).unwrap();
    }
}

impl Scheduler {
    pub fn spawn(pqueues: Vec<DiskPQueueReceiver>, config: Config, monitor: MonitorHandle) -> (JoinHandle<()>, SchedulerHandle) {
        let (empty_sender, empty_receiver) = channel();
        let (register_sender, register_receiver) = channel();
        let mut scheduler = Scheduler {
            monitor,
            empty_receiver,
            register_receiver,
            pqueues,
            config,
            work_senders: Vec::new(),
            tid_localities: Vec::new(),
            recent_domains: BTreeMap::new(),
            stashed_job: None,
            pqueue_id: 0,
        };
        let handle = SchedulerHandle {
            empty_sender,
            register_sender,
        };
        let thread_handle = thread::spawn(move || scheduler.run());
        (thread_handle, handle)
    }

    fn run(&mut self) {
        loop {
            while let Ok((work_sender, tid_sender)) = self.register_receiver.try_recv() {
                let tid = self.work_senders.len();
                self.work_senders.push(work_sender);
                self.tid_localities.push(Vec::new());
                tid_sender.send(tid).unwrap();
            }

            if let Some(job) = self.pop_job() {
                if let None = self.try_assign(job.clone()) {
                    match self.empty_receiver.try_recv() {
                        Ok(tid) => {
                            if self.work_senders[tid].len() < self.config.work_empty_threshold {
                                self.assign_job(tid, job)
                            } else {
                                self.stash_job(job);
                            }
                        },
                        Err(_) => {
                            self.stash_job(job);
                            self.monitor.inc_scheduler_free();
                            sleep(self.config.scheduler_sleep);
                        }
                    }
                }
            } else {
                self.monitor.inc_scheduler_free();
                self.monitor.inc_missing_job();
                sleep(self.config.scheduler_sleep);
            }
        }
    }

    fn assign_job(&mut self, tid: usize, job: Job) {
        self.add_locality(tid, job.locality());
        self.work_senders[tid].send(job).unwrap();
        let Self { ref mut recent_domains, ref mut tid_localities, ref config, .. } = *self;
        if thread_rng().gen::<f32>() < config.locality_clear_prob {
            for locality in &tid_localities[tid] {
                Self::remove_locality(recent_domains, tid, locality);
            }
            tid_localities[tid].clear();
        }
    }

    fn add_locality(&mut self, tid: usize, locality: JobLocality) {
        match self.recent_domains.get_mut(&locality) {
            Some(tids) => if !tids.contains(&tid) {
                tids.push(tid);
                self.tid_localities[tid].push(locality);
            },
            None => {
                self.tid_localities[tid].push(locality.clone());
                self.recent_domains.insert(locality, vec![tid]);
            },
        }
    }

    fn remove_locality(recent_domains: &mut BTreeMap<JobLocality, Vec<usize>>, tid: usize, locality: &JobLocality) {
        let tids = recent_domains.get_mut(locality).unwrap();
        for i in 0..tids.len() {
            if tids[i] == tid {
                tids.remove(i);
                break;
            }
        }
    }

    fn pop_job(&mut self) -> Option<Job> {
        let Self { ref mut stashed_job, ref mut pqueues, ref mut pqueue_id, .. } = *self;
        stashed_job.take()
            .or_else(|| {
                let res = (&mut pqueues[*pqueue_id]).pop();
                *pqueue_id = (*pqueue_id + 1) % pqueues.len();
                res
             })
    }

    fn stash_job(&mut self, job: Job) {
        assert_eq!(self.stashed_job, None);
        self.stashed_job = Some(job);
    }

    fn try_assign(&mut self, job: Job) -> Option<()> {
        match self.recent_domains.get(&job.locality()) {
            Some(tids) => tids.iter()
                .map(|tid| *tid)
                .filter(|tid| self.work_senders[*tid].len() < self.config.work_queue_cap)
                .next()
                .map(|tid| self.assign_job(tid, job)),
            // None if !self.work_senders.is_empty() => {
            //     let tid = thread_rng().gen::<usize>() % self.work_senders.len();
            //     Some(self.assign_job(tid, job))
            // },
            _ => None,
        }
    }
}
