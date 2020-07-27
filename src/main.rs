use ::web_crawler::config::Config;
use ::web_crawler::job::Job;
use ::web_crawler::workchannel::work_channel;
use ::web_crawler::index::Index;
use ::web_crawler::robots::RobotsChecker;
use ::web_crawler::scheduler::{Scheduler, SchedulerHandle};
use ::web_crawler::pqueue::{DiskPQueue, DiskPQueueSender};
use ::web_crawler::client::Client;
use ::web_crawler::monitor::{Monitor, MonitorHandle};

use std::sync::{Arc, Mutex};
use regex::Regex;
use core_affinity;
use std::thread;
use std::error::Error;
use std::time::Instant;
use std::collections::BTreeMap;
use tokio::runtime;
use tokio::time::delay_for;
use http::{Uri, HeaderMap};
use url::Url;
use rand::Rng;
use rand::thread_rng;

enum JobStatus {
    Success,
    Skipped,
}

struct Crawler {
    config: Config,
    index: Arc<Mutex<Index>>,
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
        index: Arc<Mutex<Index>>,
        robots: Arc<RobotsChecker>,
        scheduler: SchedulerHandle,
        pqueue: DiskPQueueSender,
        monitor: MonitorHandle,
    ) -> Crawler {
        let client = Client::new(config.user_agent.clone(), config.client_refresh_interval);
        Crawler {
            config,
            index,
            robots,
            scheduler,
            pqueue,
            monitor,
            client,
            link_re: Regex::new("href=['\"][^'\"]+['\"]").unwrap(),
            body_re: Regex::new(r"(?s)<body[^<>]*>.*(</body>|<script>)?").unwrap(),
            tag_text_re: Regex::new(r">([^<>]+)").unwrap(),
            term_re: Regex::new(r"[a-zA-Z]+").unwrap(),
        }
    }

    pub async fn crawl(&mut self) {
        let (work_sender, work_receiver) = work_channel();
        let tid = self.scheduler.register(work_sender);

        loop {
            let maybe_job = work_receiver.try_recv();
            match maybe_job {
                Some(job) => {
                    match self.do_job(job.clone()).await {
                        Ok(JobStatus::Success) => self.monitor.inc_successful_jobs(),
                        Ok(JobStatus::Skipped) => self.monitor.inc_skipped_jobs(),
                        Err(err) => {
                            self.monitor.inc_failed_jobs();
                            if thread_rng().gen::<f32>() < 0.02 {
                                println!("error crawling {}: {:?}", job.url, err);
                            }
                        }
                    }
                    self.monitor.inc_completed_jobs();
                },
                None => {
                    self.scheduler.mark_empty(tid);
                    self.monitor.inc_empty();
                    delay_for(self.config.crawler_empty_delay).await;
                },
            }
        }
    }

    fn headers_not_html(headers: &HeaderMap) -> bool {
        if let Some(Ok(content_type)) = headers.get("Content-Type").map(|h| h.to_str()) {
            if !content_type.starts_with("text/html") {
                return true;
            }
        }
        return false;
    }

    fn clearly_not_html(url: &str) -> bool {
        url.ends_with(".css") ||
            url.ends_with(".js") ||
            url.ends_with(".mp4") ||
            url.ends_with(".m4v") ||
            url.ends_with(".mov") ||
            url.ends_with(".dmg") ||
            url.ends_with(".pt") ||
            url.ends_with(".vdi") ||
            url.ends_with(".ova") ||
            url.ends_with(".m2ts") ||
            url.ends_with(".rmvb") ||
            url.ends_with(".npz") ||
            url.ends_with(".mat") ||
            url.ends_with(".data") ||
            url.ends_with(".7z") ||
            url.ends_with(".gz") ||
            url.ends_with(".gztar") ||
            url.ends_with(".pdf") ||
            url.ends_with(".png") ||
            url.ends_with(".PNG") ||
            url.ends_with(".ico") ||
            url.ends_with(".ICO") ||
            url.ends_with(".jpg") ||
            url.ends_with(".JPG") ||
            url.ends_with(".gif") ||
            url.ends_with(".GIF") ||
            url.ends_with(".svg") ||
            url.ends_with(".SVG") ||
            url.ends_with(".json") ||
            !url.starts_with("http")
    }

    async fn do_job(&mut self, job: Job) -> Result<JobStatus, Box<dyn Error>> {
        if thread_rng().gen::<f32>() < 0.005 {
            println!("crawling {}", job.url);
        }
        let url = Url::parse(&job.url).unwrap();
        if !self.robots.allowed(&url, &mut self.client).await {
            return Ok(JobStatus::Skipped);
        }

        let start = Instant::now();
        let res = self.client.get(url.clone()).await?;
        let final_url = res.url().clone();
        let headers = res.headers();
        if Self::headers_not_html(&headers) {
            return Ok(JobStatus::Skipped);
        }
        if let Some(content_length) = res.content_length() {
            if content_length > 100_000_000 {
                println!("megawebsite of length {}: {}", job.url, content_length);
            }
        }

        let document = Client::read_capped_bytes(res, self.config.max_document_len).await;
        self.monitor.inc_response_time(start.elapsed().as_millis());
        let document = String::from_utf8_lossy(&document);

        self.add_links(&final_url, &document);
        self.index_document(job, &document);

        Ok(JobStatus::Success)
    }

    fn add_links(&self, base_url: &Url, document: &str) {
        let links = self.link_re.find_iter(document)
            .map(|m| m.as_str())
            .map(|s| &s[6..s.len() - 1])
            .filter_map(|href| base_url.join(href).ok())
            .filter(|url| url.host_str().is_some())
            .map(|mut url| {
                url.set_fragment(None);
                url.set_query(None);
                url.into_string()
            })
            .filter(|url| !Self::clearly_not_html(url))
            .filter(|url| url.len() <= self.config.max_url_len)
            .filter(|url| url.parse::<Uri>().is_ok())
            .collect::<Vec<_>>();

        for link in links {
            self.pqueue.increment(Job::new(link));
        }
    }

    fn index_document(&self, job: Job, document: &str) -> Option<()> {
        let mut terms = BTreeMap::<String, u32>::new();
        let body = self.body_re.find(document)?.as_str();
        let mut n_terms: u32 = 0;
        for tag_text in self.tag_text_re.captures_iter(body) {
            for term in self.term_re.find_iter(&tag_text[1]) {
                let term = term.as_str().to_lowercase();
                *terms.entry(term).or_insert(0) += 1;
                n_terms += 1;
            }
        }

        let terms = terms.into_iter()
            .map(|(term, count)| (term, std::cmp::min((count * 2550) / n_terms, 255) as u8))
            .collect::<BTreeMap<_, _>>();
        let n_terms = (n_terms as f32).log2() as u8;

        self.index.lock().unwrap().insert(job, n_terms, terms);
        Some(())
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
    let robots = Arc::new(RobotsChecker::new(config.clone(), monitor.clone()));

    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    for _ in 1..config.n_threads {
        let mut crawler = Crawler::new(
            config.clone(),
            index.clone(),
            robots.clone(),
            scheduler.clone(),
            pqueue.clone(),
            monitor.clone(),
        );
        rt.spawn(async move { crawler.crawl().await });
    }
    let mut crawler = Crawler::new(
        config,
        index,
        robots,
        scheduler,
        pqueue,
        monitor,
    );
    rt.block_on(async move { crawler.crawl().await });
}

fn main() {
    let config = Config::load().unwrap();
    let monitor_handle = Monitor::spawn();
    let (pqueue_sender, pqueue_receiver) = DiskPQueue::spawn(config.clone(), monitor_handle.clone());
    for url in &config.root_set {
        pqueue_sender.increment(Job::new(String::from(*url)));
    }
    let (scheduler_thread, scheduler_handle) = Scheduler::spawn(pqueue_receiver, config.clone());

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

    scheduler_thread.join().unwrap();
}
