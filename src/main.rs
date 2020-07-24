struct Crawler {
    config: Config,
    index: Arc<Index>,
    robots: Arc<RobotsChecker>,
    scheduler: SchedulerHandle,
    pqueue: DiskPQueueSender,
    monitor: MonitorHandle,
    client: Client,
    link_re: Regex,
    body_re: Regex,
    tag_text_re: Regex,
    term_re: Regex,
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
            client: Client::new(config.client_refresh_interval),
            link_re: Regex::new("href=['\"][^'\"]+['\"]").unwrap(),
            body_re: Regex::new(r"(?s)<body[^<>]*>.*(</body>|<script>)?").unwrap(),
            tag_text_re: Regex::new(r">([^<>]+)").unwrap(),
            term_re: Regex::new(r"[a-zA-Z]+").unwrap(),
        }
    }

    pub async fn crawl(&self) {
        let (work_sender, work_receiver) = work_channel();
        let tid = self.scheduler.register(work_sender);

        loop {
            match work_receiver.try_recv() {
                Some(job) => {
                    match self.do_job(job).await {
                        Ok(JobStatus::Success) => self.monitor.inc_successful_jobs(),
                        Ok(JobStatus::Skipped) => self.monitor.inc_skipped_jobs(),
                        Err(err) => {
                            self.monitor.inc_failed_jobs();
                            if tid % 100 == 0 {
                                println!("error crawling {}: {:?}", job.url, err);
                            }
                        }
                    }
                    self.monitor.inc_completed_jobs();
                },
                None => {
                    self.scheduler.mark_empty(tid);
                    delay_for(config.crawler_empty_delay).await;
                },
            }
        }
    }

    async fn do_job(&self, job: Job) -> Result<JobStatus, Box<dyn Error>> {
        let url = Url::parse(job.url).unwrap();
        if !self.robots.allowed(&self.client, url).await {
            return Ok(JobStatus::Skipped);
        }

        let start = Instant::now();
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

        let document = Client::read_capped_bytes(res, self.config.max_document_len);
        self.monitor.inc_response_time(start.elapsed().as_millis());
        let document = String::from_utf8_lossy(document);

        self.add_links(&url, &document);
        self.index_document(&job, &document);

        Ok(JobStatus::Success)
    }

    fn add_links(&self, base_url: &Url, document: &str) {
        let links = self.link_re.find_iter(document)
            .map(|m| m.as_str())
            .map(|s| &s[6..s.len() - 1])
            .filter_map(|href| base_url.join(href).ok())
            .map(|mut url| {
                url.set_fragment(None);
                url.set_query(None);
                url.as_str()
            })
            .filter(|url| !clearly_not_html(url))
            .filter(|url| url.len <= self.config.max_url_len)
            .filter(|url| url.host_str().is_some())
            .collect::<Vec<_>>();

        for link in links {
            self.pqueue.increment(Job::new(link));
        }
    }

    fn index_document(&self, job: &Job, document: &str) {
        let mut terms = BTreeMap::<String, u32>::new();
        let body = self.body_re.find(document)?.as_str();
        let mut n_terms: u32 = 0;
        for tag_text in self.tag_text_re.captures_iter(body) {
            for term in self.term_re.find_iter(&tag_text[1]) {
                let term = term.as_str().to_lowercase();
                *terms.entry(term).or_default(0) += 1;
                n_terms += 1;
            }
        }

        let terms = terms.into_iter()
            .map(|(term, count)| (term, std::cmp::min((count * 2550) / n_terms, 255) as u8))
            .collect::<BTreeMap<_, _>>();

        self.index.insert(job, n_terms, terms);
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
    let robots = Arc::new(RobotsChecker::new(monitor.clone()));

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
