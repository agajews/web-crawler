use crossbeam::deque::{Injector, Stealer, Worker, Steal};
use lazy_static::lazy_static;
use std::error::Error;
// use std::io;
// use std::io::ErrorKind;
use reqwest::Client;
use reqwest::redirect::Policy;
// use select::document::Document;
// use select::predicate::Name;
use url::Url;
use std::iter;
use std::thread;
use std::sync::{Arc, Mutex};
use std::env;
use std::collections::BTreeMap;
use regex::Regex;
// use uuid::Uuid;
// use std::path::PathBuf;
use core::sync::atomic::{AtomicUsize, Ordering};
// use web_index::{Meta, Index};
use std::time::{Instant, Duration};
use cbloom;
use fasthash::metro::hash64;
use tokio;
use tokio::task::yield_now;
use rand;
use rand::Rng;
use rand::seq::SliceRandom;
use http::Uri;

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

fn add_links(
    source: &Url,
    document: &str,
    local: Arc<Mutex<Vec<Worker<String>>>>,
    seen: &cbloom::Filter,
    academic_re: &Regex,
    link_re: &Regex,
    total_counter: Arc<AtomicUsize>,
) {
    // TODO: link loops
    let locals = local.lock().unwrap();
    // let links = document
    //     .find(Name("a"))
    //     .filter_map(|node| node.attr("href"))
    //     .filter_map(|href| source.join(href).ok())
    //     .filter(|href| href.scheme().starts_with("http"))
    //     .filter(is_academic);
    let links = link_re.find_iter(document)
        .map(|m| m.as_str())
        .map(|s| &s[6..s.len() - 1])
        .filter_map(|href| source.join(href).ok())
        .filter(|url| is_academic(url, academic_re));
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
        if !seen.maybe_contains(h) {
            seen.insert(h);
            if let Some(host) = url.host_str() {
                let d = hash64(&host);
                let local = &locals[d as usize % locals.len()];
                local.push(url.into_string());
                total_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn crawl_url(
    url: &str,
    _id: u32,
    client: &Client,
    _meta: &mut BTreeMap<u32, (u32, String)>,
    _index: &mut BTreeMap<String, (u32, u32)>,
    locals: Arc<Mutex<Vec<Worker<String>>>>,
    seen: &cbloom::Filter,
    academic_re: &Regex,
    link_re: &Regex,
    time_counter: Arc<AtomicUsize>,
    total_counter: Arc<AtomicUsize>,
) -> Result<(), Box<dyn Error>> {
    url.parse::<Uri>()?;
    let head = client.head(url).send().await?;
    let headers = head.headers();
    if let Some(content_type) = headers.get("Content-Type") {
        let content_type = content_type.to_str()?;
        if !content_type.starts_with("text/html") {
            // return Err(Box::new(io::Error::new(ErrorKind::Other, format!("not HTML, content-type: {}", content_type))));
            return Ok(());
        }
    }

    let start = Instant::now();
    let res = client.get(url)
        .send()
        .await?;
    time_counter.fetch_add(start.elapsed().as_millis() as usize, Ordering::Relaxed);
    let res = res.text().await?;

    // let document = Document::from(res.as_str());
    // let mut terms = BTreeMap::new();
    // count_terms(&document, &mut terms);
    // let n_terms = terms
    //     .iter()
    //     .map(|(_term, count)| count)
    //     .sum();

    // meta.insert(id, (n_terms, String::from(url)));
    // for (term, count) in terms {
    //     index.insert(term, (id, count));
    // }

    let source = Url::parse(&url)?;
    if is_academic(&source, academic_re) {
        add_links(&source, res.as_str(), locals, seen, academic_re, link_re, total_counter);
    }

    Ok(())
}

async fn crawler(
    tid: usize,
    locals: Arc<Mutex<Vec<Worker<String>>>>,
    global: Arc<Injector<String>>,
    stealers: Vec<Stealer<String>>,
    seen: Arc<cbloom::Filter>,
    time_counter: Arc<AtomicUsize>,
    total_counter: Arc<AtomicUsize>,
    url_counter: Arc<AtomicUsize>,
    err_counter: Arc<AtomicUsize>,
) {
    let mut meta = BTreeMap::new();
    let mut index = BTreeMap::new();
    let client = Client::builder()
        .user_agent("Rustbot/0.1")
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .redirect(Policy::limited(100))
        .timeout(Duration::from_secs(60))
        .build().unwrap();
    let academic_re = ACADEMIC_RE.clone();
    let link_re = LINK_RE.clone();

    for id in 0.. {
        let url = loop {
            let task = find_task(&locals.lock().unwrap(), &global, &stealers);
            match task {
                Some(url) => break url,
                None => yield_now().await,
            };
        };
        if tid < 10 {
            println!("thread {} crawling {}...", tid, url);
        }
        let res = crawl_url(&url, id as u32, &client, &mut meta, &mut index, locals.clone(), &seen, &academic_re, &link_re, time_counter.clone(), total_counter.clone()).await;
        if tid < 10 {
            let n_empty = locals.lock().unwrap().iter().filter(|w| w.is_empty()).count();
            println!("thread {} finished {}, empty queues: {}", tid, url, n_empty);
        }
        if let Err(err) = res {
            if tid < 10 {
                println!("error crawling {}: {:?}", url, err);
            }
            err_counter.fetch_add(1, Ordering::Relaxed);
        }
        url_counter.fetch_add(1, Ordering::Relaxed);
    }
    println!("all done!");
}

fn find_task<T>(
    locals: &[Worker<T>],
    global: &Injector<T>,
    stealers: &[Stealer<T>],
) -> Option<T> {
    if rand::thread_rng().gen::<f32>() < 0.01 {
        let nonempty = stealers.iter().filter(|w| !w.is_empty()).collect::<Vec<_>>();
        let stealer = nonempty.choose(&mut rand::thread_rng());
        return stealer.and_then(|s| s.steal().success());
    }
    // Pop a task from the local queue, if not empty.
    let nonempty = locals.iter().filter(|w| !w.is_empty()).collect::<Vec<_>>();
    let local = nonempty.choose(&mut rand::thread_rng());
    local.and_then(|l| l.pop()).or_else(|| {
        // println!("waiting for work...");
        // Otherwise, we need to look for a task elsewhere.
        // Try stealing a batch of tasks from the global queue.
        global.steal_batch_and_pop(&locals[0])
            // Or try stealing a task from one of the other threads.
            .or_else(|| {
                let nonempty = stealers.iter().filter(|w| !w.is_empty()).collect::<Vec<_>>();
                let stealer = nonempty.choose(&mut rand::thread_rng());
                stealer.map(|s| s.steal()).unwrap_or(Steal::Empty)
            })
            .success()
        // Loop while no task was stolen and any steal operation needs to be retried.
    })
    // local.pop().or_else(|| global.steal_batch_and_pop(local).success())
}

async fn url_monitor(time_counter: Arc<AtomicUsize>, total_counter: Arc<AtomicUsize>, url_counter: Arc<AtomicUsize>, err_counter: Arc<AtomicUsize>) {
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

#[tokio::main]
async fn main() {
    let time_counter = Arc::new(AtomicUsize::new(0));
    let total_counter = Arc::new(AtomicUsize::new(0));
    let url_counter = Arc::new(AtomicUsize::new(0));
    let err_counter = Arc::new(AtomicUsize::new(0));
    let args = env::args().collect::<Vec<_>>();
    let n_threads: usize = args[1].parse().unwrap();
    let n_queues: usize = args[2].parse().unwrap();
    // let n_os_threads: usize = args[3].parse().unwrap();
    let global = Arc::new(Injector::new());
    let seen = Arc::new(cbloom::Filter::new(BLOOM_BYTES, EST_URLS));
    for url in &ROOT_SET {
        global.push(String::from(*url));
    }
    let workers = iter::repeat_with(|| {
        iter::repeat_with(|| Worker::new_fifo())
            .take(n_queues)
            .collect::<Vec<_>>()
    }).take(n_threads).collect::<Vec<_>>();
    let stealers = workers.iter()
        .flatten()
        .map(|w| w.stealer())
        .collect::<Vec<_>>();
    // let mut rt = tokio::runtime::Builder::new()
    //     .threaded_scheduler()
    //     .enable_time()
    //     .enable_io()
    //     .core_threads(n_os_threads)
    //     .max_threads(n_os_threads)
    //     .build()
    //     .unwrap();
    let monitor_handle = {
        let time_counter = time_counter.clone();
        let total_counter = total_counter.clone();
        let url_counter = url_counter.clone();
        let err_counter = err_counter.clone();
        tokio::spawn(async move { url_monitor(time_counter, total_counter, url_counter, err_counter).await })
    };
    let _threads = workers.into_iter().enumerate().map(|(tid, locals)| {
        if (tid + 1) % 100 == 0 {
            println!("spawned thread {}", tid + 1);
            // thread::sleep(Duration::from_secs(1));
        }
        let locals = Arc::new(Mutex::new(locals));
        let global = global.clone();
        let stealers = stealers.clone();
        let time_counter = time_counter.clone();
        let total_counter = total_counter.clone();
        let url_counter = url_counter.clone();
        let err_counter = err_counter.clone();
        let seen = seen.clone();
        tokio::spawn(async move { crawler(tid, locals, global, stealers, seen, time_counter, total_counter, url_counter, err_counter).await })
    }).collect::<Vec<_>>();

    println!("finished spawning threads");
    let _res = tokio::join!(monitor_handle);
}
