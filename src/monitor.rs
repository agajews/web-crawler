use std::sync::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct MonitorHandle {
    successful_jobs: Arc<AtomicUsize>,
    skipped_jobs: Arc<AtomicUsize>,
    failed_jobs: Arc<AtomicUsize>,
    completed_jobs: Arc<AtomicUsize>,
    seen_urls: Arc<AtomicUsize>,
    response_time: Arc<AtomicUsize>,
    robots_hits: Arc<AtomicUsize>,
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

    pub fn inc_completed_jobs(&self) {
        self.completed_jobs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_seen_urls(&self) {
        self.seen_urls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_response_time(&self) {
        self.response_time.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_robots_hits(&self) {
        self.robots_hits.fetch_add(1, Ordering::Relaxed);
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
            seen_urls: Arc::new(AtomicUsize::new(0)),
            response_time: Arc::new(AtomicUsize::new(0)),
            robots_hits: Arc::new(AtomicUsize::new(0)),
        }
        let monitor = Monitor {
            handle: handle.clone(),
        };
        thread::spawn(monitor.run());
        handle
    }

    fn run(&self) {
        let mut old_time = self.handle.response_time.load(Ordering::Relaxed);
        let mut old_successful = self.handle.successful_jobs.load(Ordering::Relaxed);
        let mut old_skipped = self.handle.skipped_jobs.load(Ordering::Relaxed);
        let mut old_completed = self.handle.completed_jobs.load(Ordering::Relaxed);
        let mut old_failed = self.handle.failed_jobs.load(Ordering::Relaxed);
        let mut old_robots_hits = self.handle.robots_hit.load(Ordering::Relaxed);
        println!("monitoring crawl rate");
        loop {
            thread::sleep(Duration::from_millis(1000));
            let new_time = self.handle.response_time.load(Ordering::Relaxed);
            let new_successful = self.handle.successful_jobs.load(Ordering::Relaxed);
            let new_skipped = self.handle.skipped_jobs.load(Ordering::Relaxed);
            let new_completed = self.handle.completed_jobs.load(Ordering::Relaxed);
            let new_failed = self.handle.failed_jobs.load(Ordering::Relaxed);
            let new_robots_hits = self.handle.robots_hit.load(Ordering::Relaxed);
            println!(
                "{} urls/s, {}% errs, {}% skipped, {}ms responses, {}% robot hits, crawled {}, seen {}",
                 new_successful - old_successful,
                 (new_failed - old_failed) as f32 / (new_completed - old_completed) as f32 * 100.0,
                 (new_skipped - old_skipped) as f32 / (new_completed - old_completed) as f32 * 100.0,
                 (new_time - old_time) as f32 / (new_successful - old_successful) as f32,
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
        }
    }
}
