struct Crawler {
    config: Config,
    index: Arc<Index>,
    robots: Arc<RobotsChecker>,
    scheduler: SchedulerHandle,
    pqueue: DiskPQueueSender,
    monitor: MonitorHandle,
    client: Client,
}

impl Crawler {
    pub fn new(
        config: Config,
        index: Arc<Index>,
        robots: Arc<RobotsChecker>,
        scheduler: SchedulerHandle,
        pqueue: DiskPQueueSender,
        monitor: MonitorHandle,
    ) -> Crawler {
        Crawler {
            config,
            index,
            robots,
            scheduler,
            pqueue,
            monitor,
            client: Client::new(),
        }
    }

    pub async fn crawl(&self) {
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

    async fn do_job(&self, job: Job) -> Result<JobStatus, Box<dyn Error>> {
        let url = Url::parse(job.url).unwrap();
        if !self.robots.allowed(&self.client, url) {
            return Ok(JobStatus::Skipped);
        }

        let res = self.client.get(url).await?;
        let headers = res.headers();
        if self.headers_not_html(&headers) {
            return Ok(JobStatus::Skipped);
        }
        if let Some(content_length) = res.content_length() {
            if content_length > 100_000_000 {
                println!("megawebsite of length {}: {}", job.url, content_length);
            }
        }

        let document = client.read_capped_bytes(self.config.max_document_len);
        let document = String::from_utf8_lossy(document);

        self.add_links(&document);
        self.index_document(&document);

        Ok(JobStatus::Success)
    }
}

fn core_thread(
    core_id: usize,
    config: Config,
    scheduler: SchedulerHandle,
    pqueue: DiskPQueueSender,
    monitor: MonitorHandle,
) {
    let index = Arc::new(Index::new(core_id, config.clone()));
    let robots = Arc::new(RobotsChecker::new());

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
            robots.clone(),
            scheduler.clone(),
            pqueue.clone(),
            monitor.clone(),
        );
        rt.spawn(crawler.crawl());
    }
    let crawler = Crawler::new(
        config,
        index,
        robots,
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
