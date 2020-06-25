use lazy_static::lazy_static;
use std::error::Error;
// use std::io;
// use std::io::ErrorKind;
use reqwest::Client;
use reqwest::redirect::Policy;
// use select::document::Document;
// use select::predicate::Name;
use url::Url;
use std::thread;
use std::sync::{Arc, Mutex};
use std::env;
use std::collections::VecDeque;
use regex::Regex;
// use uuid::Uuid;
// use std::path::PathBuf;
use core::sync::atomic::{AtomicUsize, Ordering};
// use web_index::{Meta, Index};
use std::time::{Instant, Duration};
use cbloom;
use fasthash::metro::hash64;
use tokio;
use tokio::time::delay_for;
use tokio::runtime;
use http::Uri;
use futures::stream::{FuturesUnordered, StreamExt};
use core_affinity;
use core_affinity::CoreId;

// variables to set:
// META_DIR - directory of metadata databases
// INDEX_DIR - directory of document indexes

const ROOT_SET: [&str; 4] = [
    "https://columbia.edu",
    "https://harvard.edu",
    "https://stanford.edu",
    "https://cam.ac.uk",
];

const BLOOM_BYTES: usize = 10_000_000_000;
const EST_URLS: usize = 1_000_000_000;

lazy_static! {
    static ref ACADEMIC_RE: Regex = Regex::new(r"^.+\.(edu|ac\.??)$").unwrap();
    static ref LINK_RE: Regex = Regex::new("href=['\"][^'\"]+['\"]").unwrap();
}

struct TaskPool<T> {
    pool: Arc<Vec<Mutex<VecDeque<T>>>>,
}

struct TaskHandler<T> {
    pool: Arc<Vec<Mutex<VecDeque<T>>>>,
    i: usize,
}

impl<T> TaskPool<T> {
    fn new(n: usize) -> TaskPool<T> {
        let mut pool = Vec::new();
        for _ in 0..n {
            pool.push(Mutex::new(VecDeque::new()));
        }
        TaskPool { pool: Arc::new(pool) }
    }

    fn handler(&self, i: usize) -> TaskHandler<T> {
        TaskHandler { pool: self.pool.clone(), i }
    }

    fn push(&self, task: T) {
        self.pool[0].lock().unwrap().push_back(task);
    }
}

impl<T> TaskHandler<T> {
    fn pop(&self) -> Option<T> {
        let own_task = {
            self.pool[self.i].try_lock().ok()?.pop_front()
        };
        match own_task {
            Some(task) => Some(task),
            None => self.steal(),
        }
    }

    fn steal(&self) -> Option<T> {
        ((self.i + 1)..=(self.i + self.pool.len()))
            .find_map(|j| self.try_steal(j))
    }

    fn try_steal(&self, j: usize) -> Option<T> {
        self.pool[j % self.pool.len()].try_lock().ok()?.pop_front()
    }

    fn push(&self, task: T) {
        self.pool[self.i].lock().unwrap().push_back(task);
    }
}

fn is_academic(url: &Url, academic_re: &Regex) -> bool {
    url.domain()
        .map(|domain| academic_re.is_match(domain))
        .unwrap_or(false)
}

// fn tokenize(document: &Document) -> Vec<String> {
//     let text = match document.find(Name("body")).next() {
//         Some(body) => body.text(),
//         None => String::from(""),
//     };
//     let mut tokens: Vec<String> = text.split_whitespace()
//         .map(String::from)
//         .collect();

//     for token in &mut tokens {
//         token.retain(|c| c.is_ascii_alphabetic());
//         token.make_ascii_lowercase();
//     }

//     tokens
//         .into_iter()
//         .filter(|t| t.len() > 0)
//         .collect()
// }

// fn count_terms(
//     document: &Document,
//     terms: &mut BTreeMap<String, u32>
// ) {
//     for token in tokenize(document) {
//         let count = match terms.get(&token) {
//             Some(count) => count.clone(),
//             None => 0,
//         };
//         terms.insert(token, count + 1);
//     }
// }

fn clearly_not_html(url: &str) -> bool {
    url.ends_with(".css") ||
        url.ends_with(".js") ||
        url.ends_with(".pdf") ||
        url.ends_with(".png") ||
        url.ends_with(".PNG") ||
        url.ends_with(".ico") ||
        url.ends_with(".ICO") ||
        url.ends_with(".jpg") ||
        url.ends_with(".JPG") ||
        url.ends_with(".gif") ||
        url.ends_with(".GIF")
}

fn add_links(source: &Url, document: &str, state: &CrawlerState) {
    let links = state.link_re.find_iter(document)
        .map(|m| m.as_str())
        .map(|s| &s[6..s.len() - 1])
        .filter_map(|href| source.join(href).ok())
        .filter(|url| is_academic(url, &state.academic_re));
    for mut url in links {
        url.set_fragment(None);
        url.set_query(None);
        if clearly_not_html(url.as_str()) {
            continue;
        }
        if url.as_str().len() > 300 {
            continue;
        }
        let h = hash64(url.as_str());
        if !state.seen.maybe_contains(h) {
            state.seen.insert(h);
            state.handler.push(url.into_string());
            state.total_counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn crawl_url(url: &str, state: &CrawlerState) -> Result<(), Box<dyn Error>> {
    url.parse::<Uri>()?;
    let head = state.client.head(url).send().await?;
    let headers = head.headers();
    if let Some(content_type) = headers.get("Content-Type") {
        let content_type = content_type.to_str()?;
        if !content_type.starts_with("text/html") {
            return Ok(());
        }
    }

    let start = Instant::now();
    let res = state.client.get(url)
        .send()
        .await?;
    state.time_counter.fetch_add(start.elapsed().as_millis() as usize, Ordering::Relaxed);
    let res = res.text().await?;

    let source = Url::parse(url)?;
    if is_academic(&source, &state.academic_re) {
        add_links(&source, res.as_str(), state);
    }

    Ok(())
}

async fn spawn_url(url: String, state: &CrawlerState) {
    if let Err(err) = crawl_url(&url, state).await {
        state.err_counter.fetch_add(1, Ordering::Relaxed);
        if state.tid < 10 {
            println!("error crawling {}: {:?}", url, err);
        }
    }
    state.url_counter.fetch_add(1, Ordering::Relaxed);
}

async fn spawn_tasks(state: &CrawlerState) {
    let mut jobs = FuturesUnordered::new();
    loop {
        while jobs.len() < state.max_conns {
            match state.handler.pop() {
                Some(url) => { jobs.push(spawn_url(url, state)); },
                None => if jobs.len() == 0 {
                    delay_for(Duration::from_secs(1)).await;
                } else {
                    break;
                },
            }
        }
        jobs.next().await;
    }
}

struct CrawlerState {
    tid: usize,
    max_conns: usize,
    handler: TaskHandler<String>,
    seen: Arc<cbloom::Filter>,
    time_counter: Arc<AtomicUsize>,
    total_counter: Arc<AtomicUsize>,
    url_counter: Arc<AtomicUsize>,
    err_counter: Arc<AtomicUsize>,
    client: Client,
    academic_re: Regex,
    link_re: Regex,
}

fn crawler(
    coreid: CoreId,
    max_conns: usize,
    handler: TaskHandler<String>,
    seen: Arc<cbloom::Filter>,
    time_counter: Arc<AtomicUsize>,
    total_counter: Arc<AtomicUsize>,
    url_counter: Arc<AtomicUsize>,
    err_counter: Arc<AtomicUsize>,
) {
    // core_affinity::set_for_current(coreid);
    // TODO: optimize request size
    let client = Client::builder()
        .user_agent("Rustbot/0.2")
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .redirect(Policy::limited(100))
        .timeout(Duration::from_secs(60))
        .build().unwrap();
    let academic_re = ACADEMIC_RE.clone();
    let link_re = LINK_RE.clone();
    let handler = handler;

    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    let state = CrawlerState {
        tid: coreid.id,
        max_conns,
        handler,
        seen,
        time_counter,
        total_counter,
        url_counter,
        err_counter,
        client,
        academic_re,
        link_re,
    };

    rt.block_on(spawn_tasks(&state));
}

fn url_monitor(time_counter: Arc<AtomicUsize>, total_counter: Arc<AtomicUsize>, url_counter: Arc<AtomicUsize>, err_counter: Arc<AtomicUsize>) {
    let mut old_time = time_counter.load(Ordering::Relaxed);
    let mut old_count = url_counter.load(Ordering::Relaxed);
    let mut old_err = err_counter.load(Ordering::Relaxed);
    println!("monitoring crawl rate");
    loop {
        thread::sleep(Duration::from_millis(1000));
        let new_time =  time_counter.load(Ordering::Relaxed);
        let new_total = total_counter.load(Ordering::Relaxed);
        let new_count = url_counter.load(Ordering::Relaxed);
        let new_err = err_counter.load(Ordering::Relaxed);
        println!(
            "{} urls/s, {}% errs, {}ms responses, crawled {}, seen {}",
             new_count - old_count,
             (new_err - old_err) as f32 / (new_count - old_count) as f32 * 100.0,
             (new_time - old_time) as f32 / (new_count - old_count) as f32,
             new_count,
             new_total,
        );
        old_time = new_time;
        old_count = new_count;
        old_err = new_err;
    }
}

fn main() {
    let time_counter = Arc::new(AtomicUsize::new(0));
    let total_counter = Arc::new(AtomicUsize::new(0));
    let url_counter = Arc::new(AtomicUsize::new(0));
    let err_counter = Arc::new(AtomicUsize::new(0));
    let args = env::args().collect::<Vec<_>>();
    let max_conns: usize = args[1].parse().unwrap();
    let core_ids = core_affinity::get_core_ids().unwrap();
    let pool = TaskPool::new(core_ids.len());
    let seen = Arc::new(cbloom::Filter::new(BLOOM_BYTES, EST_URLS));
    for url in &ROOT_SET {
        pool.push(String::from(*url));
    }
    let monitor_handle = {
        let time_counter = time_counter.clone();
        let total_counter = total_counter.clone();
        let url_counter = url_counter.clone();
        let err_counter = err_counter.clone();
        thread::spawn(move || url_monitor(time_counter, total_counter, url_counter, err_counter) )
    };
    let _threads = core_ids.into_iter().map(|coreid| {
        println!("spawned thread {}", coreid.id + 1);
        thread::sleep(Duration::from_secs(1));
        let handler = pool.handler(coreid.id);
        let time_counter = time_counter.clone();
        let total_counter = total_counter.clone();
        let url_counter = url_counter.clone();
        let err_counter = err_counter.clone();
        let seen = seen.clone();
        thread::spawn(move || crawler(coreid, max_conns, handler, seen, time_counter, total_counter, url_counter, err_counter))
    }).collect::<Vec<_>>();

    let _res = monitor_handle.join();
}
