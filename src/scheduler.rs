pub struct Scheduler {
    empty_receiver: Receiver<usize>,
    register_receiver: Receiver<(WorkSender<Job>, Sender<usize>)>,
    work_senders: Vec<WorkSender<Job>>,
    pqueue: DiskPQueue,
    config: Config,
    recent_domains: BTreeMap<JobLocality, Vec<usize>>,
}

pub struct SchedulerHandle {
    empty_sender: Sender<usize>,
    register_sender: Sender<(WorkSender<Job>, Sender<usize>)>,
}

impl Scheduler {
    pub fn spawn(config: Config) -> (JoinHandle<()>, SchedulerHandle) {
        let (empty_sender, empty_receiver) = channel();
        let (register_sender, register_receiver) = channel();
        let pqueue = DiskPQueue::spawn(config.clone());
        let scheduler = Scheduler {
            empty_receiver,
            register_receiver,
            pqueue,
            config,
            work_senders: Vec::new(),
        };
        let handle = SchedulerHandle {
            empty_sender,
            register_sender,
        };
        let thread_handle = thread::spawn(move || scheduler.run());
        (thread, handle)
    }

    fn run(&mut self) {
        loop {
            if let Ok((work_sender, tid_sender)) = self.register_sender.try_recv() {
                let tid = self.work_senders.len();
                self.work_senders.push(work_sender);
                self.tid_localities.push(Vec::new());
                tid_sender.send(tid).unwrap();
            }

            if let Ok(tid) = self.empty_receiver.try_recv() {
                if let Some(job) = self.pop_job() {
                    self.assign_job(tid, job);
                }
            }

            let job = self.pop_job();
            if let None self.try_assign(job) {
                self.stash_job(job);
                sleep(self.config.scheduler_sleep);
            }
        }
    }

    fn assign_job(&mut self, tid: usize, job: Job) {
        self.work_senders.send(job).unwrap();
        self.add_locality(tid, job.locality());
        if self.rng.gen::<f32>() < self.config.locality_clear_prob {
            for locality in &self.tid_localities[tid] {
                self.remove_locality(tid, locality);
            }
            self.tid_localities[tid].clear();
        }
    }

    fn add_locality(&mut self, tid: usize, locality: JobLocality) {
        match self.recent_domains.get_mut(locality) {
            Some(tids) => if !tids.contains(tid) {
                tids.push(tid);
                self.tid_localities[tid].push(locality);
            },
            None => {
                self.recent_domains.insert(locality, vec![tid]);
                self.tid_localities[tid].push(locality);
            },
        }
    }

    fn remove_locality(&mut self, tid: usize, locality: JobLocality) {
        let tids = self.recent_domains.get_mut(locality).unwrap();
        for i in 0..tids.len() {
            if tids[i] == tid {
                tids.remove(i);
                break;
            }
        }
    }

    fn pop_job(&mut self) -> Option<Job> {
        if let Some(job) = self.stashed_job {
            self.stashed_job = None;
            return Some(job);
        }
        self.pqueue.pop()
    }

    fn stash_job(&mut self, job: Job) {
        assert_eq!(self.stashed_job, None);
        self.stashed_job = Some(job);
    }

    fn try_assign(&mut self, job: Job) -> Option<()> {
        let tids = self.recent_domains.get(job.locality())?;
        for tid in tids {
            if self.work_senders[tid].len() < self.config.work_queue_cap {
                self.assign_job(tid, job);
                return Some(());
            }
        }
        return None;
    }
}