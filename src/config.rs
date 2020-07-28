use std::env;
use std::time::Duration;
use std::path::PathBuf;

#[derive(Clone)]
pub struct Config {
    pub pqueue_path: PathBuf,
    pub index_path: PathBuf,
    pub index_cap: usize,
    pub max_document_len: usize,
    pub page_capacity: usize,
    pub max_url_len: usize,
    pub page_size_bytes: usize,
    pub scheduler_queue_cap: usize,
    pub n_pqueue_threads: usize,
    pub pqueue_cache_cap: usize,
    pub scheduler_sleep: Duration,
    pub pqueue_sleep: Duration,
    pub locality_clear_prob: f32,
    pub work_queue_cap: usize,
    pub min_run_len: usize,
    pub client_refresh_interval: usize,
    pub crawler_empty_delay: Duration,
    pub root_set: Vec<&'static str>,
    pub user_agent: String,
    pub n_threads: usize,
    pub n_pqueues: usize,
}

impl Config {
    pub fn load() -> Option<Config> {
        let top_dir: PathBuf = env::var("CRAWLER_DIR").ok()?.into();
        let debug = env::args().count() > 1;
        let config = Config {
            pqueue_path: top_dir.join("pqueue"),
            index_path: top_dir.join("index"),
            index_cap: if debug { 100 } else { 100_000 },
            max_document_len: 256_000,
            page_capacity: 60,
            max_url_len: 250,
            page_size_bytes: 4096 * 4,
            scheduler_queue_cap: 10,
            work_queue_cap: 100,
            n_pqueue_threads: if debug { 2 } else { 32 },
            pqueue_cache_cap: if debug { 20 } else {
                12_500_000
                // 1000
            },
            scheduler_sleep: Duration::from_millis(1),
            pqueue_sleep: Duration::from_millis(1),
            locality_clear_prob: 0.001,
            min_run_len: 32,
            client_refresh_interval: 100,
            crawler_empty_delay: Duration::from_millis(10),
            root_set: vec![
                "https://columbia.edu",
                "https://harvard.edu",
                "https://mit.edu",
                "https://cam.ac.uk",
            ],
            user_agent: String::from("Rustbot/0.4"),
            n_threads: if debug { 1 } else { 50 },
            n_pqueues: if debug { 2 } else { 16 },
        };

        Some(config)
    }
}
