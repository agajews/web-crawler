use std::env;
use std::time::Duration;
use std::path::PathBuf;

#[derive(Clone)]
pub struct Config {
    pqueue_path: PathBuf,
    index_path: PathBuf,
    index_cap: usize,
    max_document_len: usize,
    page_capacity: usize,
    max_url_len: usize,
    page_size_bytes: usize,
    scheduler_queue_cap: usize,
    n_pqueue_threads: usize,
    pqueue_cache_cap: usize,
    scheduler_sleep: Duration,
    locality_clear_prob: f32,
    work_queue_cap: usize,
    min_run_len: usize,
    client_refresh_interval: usize,
    crawler_empty_delay: Duration,
    root_set: Vec<String>,
}

impl Config {
    pub fn load() -> Option<Config> {
        let top_dir: PathBuf = env::var("CRAWLER_DIR")?.into();
        Config {
            pqueue_path: top_dir.join("pqueue"),
            index_path: top_dir.join("index"),
            index_cap: 100_000,
            max_document_len: 256_000,
            page_capacity: 60,
            max_url_len: 250,
            page_size_bytes: 4096 * 4,
            scheduler_queue_cap: 100,
            n_pqueue_threads: 256,
            pqueue_cache_cap: 12_500_000,
            scheduler_sleep: Duration::from_millis(1),
            locality_clear_prob: 0.01,
            work_queue_cap: 100,
            min_run_len: 32,
            client_refresh_interval: 100,
            crawler_empty_delay: Duration::from_millis(1),
            root_set: vec![
                "https://columbia.edu",
                "https://harvard.edu",
                "https://mit.edu",
                "https://cam.ac.uk",
            ],
        }
    }
}
