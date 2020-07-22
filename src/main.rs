struct Crawler {
    config: Config,
    index: Arc<Mutex<Index>>,
    scheduler: SchedulerHandle,
    pqueue: DiskPQueueSender,
    monitor: MonitorHandle,
}

impl Crawler {
    async fn crawl(&self) {
        let (work_sender, work_receiver) = work_channel();
        let tid = self.scheduler.register(work_sender);

        loop {
            match work_receiver.try_recv() {
                Some(job) => self.do_job(job).await,
                None => {
                    self.scheduler.mark_empty(tid);
                    delay_for(config.empty_delay).await;
                },
            }
        }
    }

    async fn do_job(&self, job: Job) {
    }
}

fn core_thread(
    core_id: usize,
    config: Config,
    scheduler: SchedulerHandle,
    pqueue: DiskPQueueSender,
    monitor: MonitorHandle,
) {
    let index = Arc::new(Mutex::new(Index::new(core_id, config.clone())));

    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    for _ in 1..config.n_threads {
        let crawler = Crawler::new(
            config.clone(),
            index.clone(),
            scheduler.clone(),
            pqueue.clone(),
            monitor.clone(),
        );
        rt.spawn(crawler.crawl());
    }
    let crawler = Crawler::new(
        config,
        index,
        scheduler,
        pqueue,
        monitor,
    );
    rt.block_on(crawler.crawl());
}

fn main() {
    let config = Config::load().unwrap();
    let (pqueue_sender, pqueue_receiver) = DiskPQueue::spawn(config.clone());
    let (scheduler_thread, scheduler_handle) = Scheduler::spawn(pqueue_receiver, config.clone());
    let monitor_handle = Monitor::spawn(config.clone());

    let core_ids = core_affinity::get_core_ids().unwrap();
    for core_id in core_ids {
        let config = config.clone();
        let scheduler_handle = scheduler_handle.clone();
        let pqueue_sender = pqueue_sender.clone();
        let monitor_handle = monitor_handle.clone();
        thread::spawn(move || {
            core_affinity::set_for_current(core_id);
            core_thread(core_id.id, config, scheduler_handle, pqueue_sender, monitor_handle)
        });
    }

    scheduler.join().unwrap();
}
