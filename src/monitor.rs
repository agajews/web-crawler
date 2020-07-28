use std::sync::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub struct MonitorHandle {
    successful_jobs: Arc<AtomicUsize>,
    skipped_jobs: Arc<AtomicUsize>,
    failed_jobs: Arc<AtomicUsize>,
    completed_jobs: Arc<AtomicUsize>,
    total_priority: Arc<AtomicUsize>,
    seen_urls: Arc<AtomicUsize>,
    response_time: Arc<AtomicUsize>,
    robots_hits: Arc<AtomicUsize>,
    empty_requests: Arc<AtomicUsize>,
    scheduler_free: Arc<AtomicUsize>,
    pqueue_free: Arc<AtomicUsize>,
    missing_job: Arc<AtomicUsize>,
}

impl MonitorHandle {
    pub fn inc_successful_jobs(&self) {
        self.successful_jobs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_skipped_jobs(&self) {
        self.skipped_jobs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed_jobs(&self) {
        self.failed_jobs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_empty(&self) {
        self.empty_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_missing_job(&self) {
        self.missing_job.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_scheduler_free(&self) {
        self.scheduler_free.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_pqueue_free(&self) {
        self.pqueue_free.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_completed_jobs(&self) {
        self.completed_jobs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_seen_urls(&self) {
        self.seen_urls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_response_time(&self, millis: u128) {
        self.response_time.fetch_add(millis as usize, Ordering::Relaxed);
    }

    pub fn inc_robots_hits(&self) {
        self.robots_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_total_priority(&self, priority: u32) {
        self.total_priority.fetch_add(priority as usize, Ordering::Relaxed);
    }
}

pub struct Monitor {
    handle: MonitorHandle,
}

impl Monitor {
    pub fn spawn() -> MonitorHandle {
        let handle = MonitorHandle {
            successful_jobs: Arc::new(AtomicUsize::new(0)),
            skipped_jobs: Arc::new(AtomicUsize::new(0)),
            failed_jobs: Arc::new(AtomicUsize::new(0)),
            completed_jobs: Arc::new(AtomicUsize::new(0)),
            total_priority: Arc::new(AtomicUsize::new(0)),
            seen_urls: Arc::new(AtomicUsize::new(0)),
            response_time: Arc::new(AtomicUsize::new(0)),
            robots_hits: Arc::new(AtomicUsize::new(0)),
            empty_requests: Arc::new(AtomicUsize::new(0)),
            scheduler_free: Arc::new(AtomicUsize::new(0)),
            pqueue_free: Arc::new(AtomicUsize::new(0)),
            missing_job: Arc::new(AtomicUsize::new(0)),
        };
        let monitor = Monitor {
            handle: handle.clone(),
        };
        thread::spawn(move || monitor.run());
        handle
    }

    fn run(&self) {
        let mut old_time = self.handle.response_time.load(Ordering::Relaxed);
        let mut old_successful = self.handle.successful_jobs.load(Ordering::Relaxed);
        let mut old_skipped = self.handle.skipped_jobs.load(Ordering::Relaxed);
        let mut old_completed = self.handle.completed_jobs.load(Ordering::Relaxed);
        let mut old_failed = self.handle.failed_jobs.load(Ordering::Relaxed);
        let mut old_robots_hits = self.handle.robots_hits.load(Ordering::Relaxed);
        let mut old_total_priority = self.handle.total_priority.load(Ordering::Relaxed);
        let mut old_empty_requests = self.handle.empty_requests.load(Ordering::Relaxed);
        let mut old_scheduler_free = self.handle.scheduler_free.load(Ordering::Relaxed);
        let mut old_pqueue_free = self.handle.pqueue_free.load(Ordering::Relaxed);
        let mut old_missing_job = self.handle.missing_job.load(Ordering::Relaxed);
        println!("monitoring crawl rate");
        loop {
            thread::sleep(Duration::from_millis(1000));
            let new_time = self.handle.response_time.load(Ordering::Relaxed);
            let new_successful = self.handle.successful_jobs.load(Ordering::Relaxed);
            let new_skipped = self.handle.skipped_jobs.load(Ordering::Relaxed);
            let new_completed = self.handle.completed_jobs.load(Ordering::Relaxed);
            let new_failed = self.handle.failed_jobs.load(Ordering::Relaxed);
            let new_robots_hits = self.handle.robots_hits.load(Ordering::Relaxed);
            let new_total_priority = self.handle.total_priority.load(Ordering::Relaxed);
            let new_empty_requests = self.handle.empty_requests.load(Ordering::Relaxed);
            let new_scheduler_free = self.handle.scheduler_free.load(Ordering::Relaxed);
            let new_pqueue_free = self.handle.pqueue_free.load(Ordering::Relaxed);
            let new_missing_job = self.handle.missing_job.load(Ordering::Relaxed);
            println!(
                "{} urls/s, {}% errs, {}% skipped, {}ms responses, {} empty, {} scheduler free, {} pqueue free, {} missing, {} avg priority, {}% robot hits, crawled {}, seen {}",
                 new_successful - old_successful,
                 (new_failed - old_failed) as f32 / (new_completed - old_completed) as f32 * 100.0,
                 (new_skipped - old_skipped) as f32 / (new_completed - old_completed) as f32 * 100.0,
                 (new_time - old_time) as f32 / (new_successful - old_successful) as f32,
                 new_empty_requests - old_empty_requests,
                 new_scheduler_free - old_scheduler_free,
                 new_pqueue_free - old_pqueue_free,
                 new_missing_job - old_missing_job,
                 (new_total_priority - old_total_priority) as f32 / (new_completed - old_completed) as f32,
                 (new_robots_hits - old_robots_hits) as f32 / (new_completed - old_completed) as f32 * 100.0,
                 new_completed,
                 self.handle.seen_urls.load(Ordering::Relaxed),
            );
            old_time = new_time;
            old_successful = new_successful;
            old_skipped = new_skipped;
            old_completed = new_completed;
            old_failed = new_failed;
            old_robots_hits = new_robots_hits;
            old_total_priority = new_total_priority;
            old_empty_requests = new_empty_requests;
            old_scheduler_free = new_scheduler_free;
            old_pqueue_free = new_pqueue_free;
            old_missing_job = new_missing_job;
        }
    }
}
