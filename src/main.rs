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
use std::collections::{BTreeMap, VecDeque};
use regex::Regex;
// use uuid::Uuid;
use std::path::{Path ,PathBuf};
use core::sync::atomic::{AtomicUsize, Ordering};
// use web_index::{Meta, Index};
use std::time::{Instant, Duration};
use cbloom;
use fasthash::metro::hash64;
use tokio;
use tokio::time::delay_for;
use tokio::runtime;
use http::Uri;
use core_affinity;
use core_affinity::CoreId;
use rand;
use rand::Rng;
use futures_util::StreamExt;
use bincode::{serialize, deserialize};
use std::fs;
use serde::Serialize;
use serde::de::DeserializeOwned;

// variables to set:
// META_DIR - directory of metadata databases
// INDEX_DIR - directory of document indexes

const USER_AGENT: &str = "Rustbot/0.3";

const ROOT_SET: [&str; 4] = [
    "https://columbia.edu",
    "https://harvard.edu",
    "https://mit.edu",
    "https://cam.ac.uk",
];

const BLOOM_BYTES: usize = 10_000_000_000;
const EST_URLS: usize = 1_000_000_000;
const SWAP_CAP: usize = 1_000;

lazy_static! {
    static ref ACADEMIC_RE: Regex = Regex::new(r"^.+\.(edu|ac\.??)$").unwrap();
    static ref LINK_RE: Regex = Regex::new("href=['\"][^'\"]+['\"]").unwrap();
    static ref DISALLOW_RE: Regex = Regex::new(r"^Disallow: ([^\s]+)").unwrap();
}

struct DiskDeque<T> {
    front: VecDeque<T>,
    back: VecDeque<T>,
    save_start: usize,
    save_end: usize,
    dir: PathBuf,
    capacity: usize,
}

impl<T: Serialize + DeserializeOwned> DiskDeque<T> {
    fn new<P: Into<PathBuf>>(dir: P, capacity: usize) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        DiskDeque {
            front: VecDeque::new(),
            back: VecDeque::new(),
            save_start: 0,
            save_end: 0,
            dir: dir,
            capacity,
        }
    }

    fn pop(&mut self) -> Option<T> {
        if let Some(x) = self.front.pop_front() {
            return Some(x);
        }
        if self.save_start < self.save_end {
            self.front = self.load_swap(self.save_start);
            self.save_start += 1;
            return self.front.pop_front();
        }
        self.back.pop_front()
    }

    fn push(&mut self, x: T) {
        self.back.push_back(x);
        if self.back.len() >= self.capacity {
            self.dump_swap(self.save_end);
            self.back.clear();
            self.save_end += 1;
        }
    }

    fn swap_path(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("swap{}", idx))
    }

    fn load_swap(&self, idx: usize) -> VecDeque<T> {
        let bytes = fs::read(self.swap_path(idx)).unwrap();
        deserialize(&bytes).unwrap()
    }

    fn dump_swap(&self, idx: usize) {
        fs::write(self.swap_path(idx), serialize(&self.back).unwrap()).unwrap()
    }
}

struct TaskPool<T> {
    pool: Arc<Vec<Mutex<DiskDeque<T>>>>,
}

struct TaskHandler<T> {
    pool: Arc<Vec<Mutex<DiskDeque<T>>>>,
    i: usize,
}

impl<T: Serialize + DeserializeOwned> TaskPool<T> {
    fn new<P: AsRef<Path>>(dir: P, capacity: usize, n: usize) -> TaskPool<T> {
        let mut pool = Vec::new();
        let dir = dir.as_ref();
        for i in 0..n {
            pool.push(Mutex::new(
                DiskDeque::new(dir.join(format!("thread{}", i)), capacity)
            ));
        }
        TaskPool { pool: Arc::new(pool) }
    }

    fn handler(&self, i: usize) -> TaskHandler<T> {
        TaskHandler { pool: self.pool.clone(), i }
    }

    fn push(&self, task: T) {
        let i = rand::thread_rng().gen::<usize>() % self.pool.len();
        self.pool[i].lock().unwrap().push(task);
    }
}

impl<T: Serialize + DeserializeOwned> TaskHandler<T> {
    fn pop(&self) -> Option<T> {
        let own_task = {
            self.pool[self.i].try_lock().ok()?.pop()
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
        self.pool[j % self.pool.len()].try_lock().ok()?.pop()
    }

    fn push(&self, task: T, d: usize) {
        // let j = rand::thread_rng().gen::<usize>() % self.pool.len();
        self.pool[d % self.pool.len()].lock().unwrap().push(task);
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

async fn fetch_robots(url: &Url, client: &Client, state: &CrawlerState) -> Option<Vec<String>> {
    let res = client.get(url.join("/robots.txt").ok()?).send().await.ok()?;
    let mut stream = res.bytes_stream();
    let mut total_len: usize = 0;
    let mut data = Vec::new();
    while let Some(Ok(chunk)) = stream.next().await {
        total_len += chunk.len();
        if total_len > 256_000 {
            return None;
        }
        data.extend_from_slice(&chunk);
    }
    let robots = String::from_utf8(data).ok()?;
    let mut prefixes = Vec::new();
    let mut should_match = false;
    for line in robots.lines() {
        if line.starts_with("User-agent: ") {
            should_match = line.starts_with("User-agent: *") || line.starts_with("User-agent: Rustbot");
            continue;
        }
        if should_match {
            match state.disallow_re.captures(line) {
                Some(caps) => match caps.get(1) {
                    Some(prefix) => prefixes.push(String::from(prefix.as_str())),
                    None => continue,
                },
                None => continue,
            }
        }
    }
    if prefixes.is_empty() {
        return None;
    }
    Some(prefixes)
}

fn match_url(url: &Url, prefixes: &Option<Vec<String>>) -> bool {
    let prefixes = match prefixes {
        Some(prefixes) => prefixes,
        None => return true,
    };
    let path = url.path();
    for prefix in prefixes {
        if path.starts_with(prefix) {
            return false;
        }
    }
    return true;
}

async fn robots_allowed(url: &Url, client: &Client, state: &CrawlerState, robots: &mut BTreeMap<String, Option<Vec<String>>>) -> bool {
    let host = match url.host_str() {
        Some(host) => host,
        None => return false,
    };
    if let Some(prefixes) = robots.get(host) {
        state.robots_hit_counter.fetch_add(1, Ordering::Relaxed);
        return match_url(url, prefixes);
    }
    let prefixes = fetch_robots(url, client, state).await;
    robots.insert(String::from(host), prefixes);
    match_url(url, robots.get(host).unwrap())
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
        !url.starts_with("http")
}

fn add_links(source: &Url, document: &str, state: &CrawlerState, handler: &TaskHandler<String>) {
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
            if let Some(host) = url.host_str() {
                let d = hash64(host);
                handler.push(url.into_string(), d as usize);
                state.total_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn crawl_url(
    url: &str,
    client: &Client,
    robots: &mut BTreeMap<String, Option<Vec<String>>>,
    state: &CrawlerState,
    handler: &TaskHandler<String>,
) -> Result<(), Box<dyn Error>> {
    url.parse::<Uri>()?;
    let source = Url::parse(url)?;

    if !robots_allowed(&source, client, state, robots).await {
        // return Err(Box::new(io::Error::new(ErrorKind::Other, "blocked by robots.txt")));
        return Ok(());
    }

    let start = Instant::now();
    let res = client.get(url)
        .send()
        .await?;
    let headers = res.headers();
    if let Some(content_type) = headers.get("Content-Type") {
        let content_type = content_type.to_str()?;
        if !content_type.starts_with("text/html") {
            // return Err(Box::new(io::Error::new(ErrorKind::Other, format!("wrong content type {}", content_type))));
            return Ok(());
        }
    }
    if let Some(content_length) = res.content_length() {
        if content_length > 100_000_000 {
            println!("megawebsite of length {}: {}", url, content_length);
        }
    }

    let should_add_links = is_academic(&source, &state.academic_re);

    let mut stream = res.bytes_stream();
    let mut total_len: usize = 0;
    while let Some(Ok(chunk)) = stream.next().await {
        total_len += chunk.len();
        if total_len > 256_000 {
            break;
        }
        if should_add_links {
            add_links(&source, std::str::from_utf8(&chunk)?, state, handler);
        }
    }

    state.time_counter.fetch_add(start.elapsed().as_millis() as usize, Ordering::Relaxed);

    Ok(())
}

fn build_client() -> Client {
    // TODO: optimize request size
    Client::builder()
        .user_agent(USER_AGENT)
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .redirect(Policy::limited(100))
        .timeout(Duration::from_secs(60))
        .build().unwrap()
}

async fn crawler(state: Arc<CrawlerState>, handler: TaskHandler<String>, _id: usize) {
    // delay_for(Duration::from_millis(100 * id as u64)).await;
    let mut n_urls = 0;
    let mut client = build_client();
    let mut robots = BTreeMap::new();
    let mut was_active = false;
    loop {
        let url = match handler.pop() {
            Some(url) => {
                if !was_active {
                    state.active_counter.fetch_add(1, Ordering::Relaxed);
                    was_active = true;
                }
                url
            },
            None => {
                if was_active {
                    state.active_counter.fetch_sub(1, Ordering::Relaxed);
                    was_active = false;
                }
                delay_for(Duration::from_millis(100)).await; continue;
            },
        };
        if let Err(err) = crawl_url(&url, &client, &mut robots, &state, &handler).await {
            state.err_counter.fetch_add(1, Ordering::Relaxed);
            if state.tid < 1 {
                println!("error crawling {}: {:?}", url, err);
            }
        }
        state.url_counter.fetch_add(1, Ordering::Relaxed);
        n_urls += 1;
        if n_urls % 100 == 0 {
            drop(client);
            client = build_client();
        }
    }
}

struct CrawlerState {
    tid: usize,
    seen: Arc<cbloom::Filter>,
    time_counter: Arc<AtomicUsize>,
    total_counter: Arc<AtomicUsize>,
    url_counter: Arc<AtomicUsize>,
    err_counter: Arc<AtomicUsize>,
    active_counter: Arc<AtomicUsize>,
    robots_hit_counter: Arc<AtomicUsize>,
    academic_re: Regex,
    link_re: Regex,
    disallow_re: Regex,
}

fn crawler_core(
    coreid: CoreId,
    max_conns: usize,
    handlers: Vec<TaskHandler<String>>,
    seen: Arc<cbloom::Filter>,
    time_counter: Arc<AtomicUsize>,
    total_counter: Arc<AtomicUsize>,
    url_counter: Arc<AtomicUsize>,
    err_counter: Arc<AtomicUsize>,
    active_counter: Arc<AtomicUsize>,
    robots_hit_counter: Arc<AtomicUsize>,
) {
    core_affinity::set_for_current(coreid);
    let academic_re = ACADEMIC_RE.clone();
    let link_re = LINK_RE.clone();
    let disallow_re = DISALLOW_RE.clone();

    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    let state = Arc::new(CrawlerState {
        tid: coreid.id,
        seen,
        time_counter,
        total_counter,
        url_counter,
        err_counter,
        active_counter,
        robots_hit_counter,
        academic_re,
        link_re,
        disallow_re,
    });

    for (id, handler) in handlers.into_iter().enumerate() {
        let state = state.clone();
        if id < max_conns - 1 {
            rt.spawn(crawler(state, handler, id));
        } else {
            rt.block_on(crawler(state, handler, id));
        }
    }

}

fn url_monitor(time_counter: Arc<AtomicUsize>, total_counter: Arc<AtomicUsize>, url_counter: Arc<AtomicUsize>, err_counter: Arc<AtomicUsize>, active_counter: Arc<AtomicUsize>, robots_hit_counter: Arc<AtomicUsize>) {
    let mut old_time = time_counter.load(Ordering::Relaxed);
    let mut old_count = url_counter.load(Ordering::Relaxed);
    let mut old_err = err_counter.load(Ordering::Relaxed);
    let mut old_robots_hits = robots_hit_counter.load(Ordering::Relaxed);
    println!("monitoring crawl rate");
    loop {
        thread::sleep(Duration::from_millis(1000));
        let new_time =  time_counter.load(Ordering::Relaxed);
        let new_total = total_counter.load(Ordering::Relaxed);
        let new_count = url_counter.load(Ordering::Relaxed);
        let new_err = err_counter.load(Ordering::Relaxed);
        let new_active = active_counter.load(Ordering::Relaxed);
        let new_robots_hits = robots_hit_counter.load(Ordering::Relaxed);
        println!(
            "{} urls/s, {}% errs, {}ms responses, {}% robot hits, {} active, crawled {}, seen {}",
             new_count - old_count,
             (new_err - old_err) as f32 / (new_count - old_count) as f32 * 100.0,
             (new_time - old_time) as f32 / (new_count - old_count) as f32,
             (new_robots_hits - old_robots_hits) as f32 / (new_count - old_count) as f32 * 100.0,
             new_active,
             new_count,
             new_total,
        );
        old_time = new_time;
        old_count = new_count;
        old_err = new_err;
        old_robots_hits = new_robots_hits;
    }
}

fn main() {
    let time_counter = Arc::new(AtomicUsize::new(0));
    let total_counter = Arc::new(AtomicUsize::new(0));
    let url_counter = Arc::new(AtomicUsize::new(0));
    let err_counter = Arc::new(AtomicUsize::new(0));
    let active_counter = Arc::new(AtomicUsize::new(0));
    let robots_hit_counter = Arc::new(AtomicUsize::new(0));
    let args = env::args().collect::<Vec<_>>();
    let max_conns: usize = args[1].parse().unwrap();
    let swap_dir = env::var("SWAP_DIR").unwrap();
    let core_ids = core_affinity::get_core_ids().unwrap();
    let pool = TaskPool::new(swap_dir, SWAP_CAP, core_ids.len() * max_conns);
    let seen = Arc::new(cbloom::Filter::new(BLOOM_BYTES, EST_URLS));
    for url in &ROOT_SET {
        pool.push(String::from(*url));
    }
    let monitor_handle = {
        let time_counter = time_counter.clone();
        let total_counter = total_counter.clone();
        let url_counter = url_counter.clone();
        let err_counter = err_counter.clone();
        let active_counter = active_counter.clone();
        let robots_hit_counter = robots_hit_counter.clone();
        thread::spawn(move || url_monitor(time_counter, total_counter, url_counter, err_counter, active_counter, robots_hit_counter) )
    };
    let _threads = core_ids.into_iter().map(|coreid| {
        println!("spawned thread {}", coreid.id + 1);
        // thread::sleep(Duration::from_secs(1));
        let mut handlers = Vec::new();
        for i in 0..max_conns {
            handlers.push(pool.handler(coreid.id * max_conns + i));
        }
        let time_counter = time_counter.clone();
        let total_counter = total_counter.clone();
        let url_counter = url_counter.clone();
        let err_counter = err_counter.clone();
        let active_counter = active_counter.clone();
        let robots_hit_counter = robots_hit_counter.clone();
        let seen = seen.clone();
        thread::spawn(move || crawler_core(coreid, max_conns, handlers, seen, time_counter, total_counter, url_counter, err_counter, active_counter, robots_hit_counter))
    }).collect::<Vec<_>>();

    let _res = monitor_handle.join();
}
