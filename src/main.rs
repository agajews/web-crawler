use crossbeam::deque::{Injector, Stealer, Worker};
use lazy_static::lazy_static;
use std::error::Error;
use reqwest::blocking::Client;
use reqwest::redirect::Policy;
use select::document::Document;
use select::predicate::Name;
use url::Url;
use std::iter;
use std::thread;
use std::sync::Arc;
use std::env;
use std::collections::BTreeMap;
use regex::Regex;
use uuid::Uuid;
use std::path::PathBuf;
use core::sync::atomic::{AtomicUsize, Ordering};
use web_index::{Meta, Index};
use std::time::Duration;
use cbloom;
use fasthash::metro::hash64;

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
    static ref ACADEMIC_RE: Regex = Regex::new(r".+\.(edu|ac\.??)").unwrap();
}

fn is_academic(url: &Url) -> bool {
    url.domain()
        .map(|domain| ACADEMIC_RE.is_match(domain))
        .unwrap_or(false)
}

fn tokenize(document: &Document) -> Vec<String> {
    let text = match document.find(Name("body")).next() {
        Some(body) => body.text(),
        None => String::from(""),
    };
    let mut tokens: Vec<String> = text.split_whitespace()
        .map(String::from)
        .collect();

    for token in &mut tokens {
        token.retain(|c| c.is_ascii_alphabetic());
        token.make_ascii_lowercase();
    }

    tokens
        .into_iter()
        .filter(|t| t.len() > 0)
        .collect()
}

fn count_terms(
    document: &Document,
    terms: &mut BTreeMap<String, u32>
) {
    for token in tokenize(document) {
        let count = match terms.get(&token) {
            Some(count) => count.clone(),
            None => 0,
        };
        terms.insert(token, count + 1);
    }
}

fn add_links(
    source: &Url,
    document: &Document,
    local: &Worker<String>,
    seen: &cbloom::Filter,
) {
    document
        .find(Name("a"))
        .filter_map(|node| node.attr("href"))
        .filter_map(|href| source.join(href).ok())
        .filter(|href| href.scheme().starts_with("http"))
        .for_each(|mut url| {
            url.set_fragment(None);
            let url = url.into_string();
            let h = hash64(&url);
            if !seen.maybe_contains(h) {
                seen.insert(h);
                local.push(url);
            }
        });
}

fn crawl_url(
    url: &str,
    id: u32,
    client: &Client,
    meta: &Meta,
    index: &Index,
    local: &Worker<String>,
    seen: &cbloom::Filter,
) -> Result<(), Box<dyn Error>>{
    let res = client.get(url)
        .send()?
        .text()?;
    let document = Document::from(res.as_str());
    let mut terms = BTreeMap::new();
    count_terms(&document, &mut terms);
    let n_terms = terms
        .iter()
        .map(|(_term, count)| count)
        .sum();

    meta.insert(id, (n_terms, String::from(url)));
    for (term, count) in terms {
        index.insert(&term, (id, count));
    }

    let source = Url::parse(&url)?;
    if is_academic(&source) {
        add_links(&source, &document, &local, &seen);
    }

    Ok(())
}

fn crawler(
    local: Worker<String>,
    global: Arc<Injector<String>>,
    stealers: Vec<Stealer<String>>,
    seen: Arc<cbloom::Filter>,
    url_counter: Arc<AtomicUsize>,
) {
    let meta = Meta::new(
        PathBuf::from(env::var("META_DIR").unwrap())
        .join(Uuid::new_v4().to_string())
    );
    let index = Index::new(
        PathBuf::from(env::var("INDEX_DIR").unwrap())
        .join(Uuid::new_v4().to_string())
    );
    let client = Client::builder()
        .user_agent("Rustbot/0.1")
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .redirect(Policy::limited(100))
        .build().unwrap();
    let urls = iter::repeat_with(|| find_task(&local, &global, &stealers))
        .filter_map(|url| url);
    for (id, url) in urls.enumerate() {
        let res = crawl_url(&url, id as u32, &client, &meta, &index, &local, &seen);
        if let Err(err) = res {
            println!("error crawling {}: {:?}", url, err);
        }
        url_counter.fetch_add(1, Ordering::Relaxed);
    }
}

fn find_task<T>(
    local: &Worker<T>,
    global: &Injector<T>,
    stealers: &[Stealer<T>],
) -> Option<T> {
    // Pop a task from the local queue, if not empty.
    local.pop().or_else(|| {
        println!("waiting for work...");
        // Otherwise, we need to look for a task elsewhere.
        iter::repeat_with(|| {
            // Try stealing a batch of tasks from the global queue.
            global.steal_batch_and_pop(local)
                // Or try stealing a task from one of the other threads.
                .or_else(|| stealers.iter().map(|s| s.steal()).collect())
        })
        // Loop while no task was stolen and any steal operation needs to be retried.
        .find(|s| !s.is_retry())
        // Extract the stolen task, if there is one.
        .and_then(|s| s.success())
    })
    // local.pop().or_else(|| global.steal_batch_and_pop(local).success())
}

fn main() {
    let url_counter = Arc::new(AtomicUsize::new(0));
    let n_threads: usize = env::args()
        .collect::<Vec<String>>()
        [1]
        .parse().unwrap();
    let global = Arc::new(Injector::new());
    let seen = Arc::new(cbloom::Filter::new(BLOOM_BYTES, EST_URLS));
    for url in &ROOT_SET {
        global.push(String::from(*url));
    }
    let workers = iter::repeat_with(|| Worker::new_fifo())
        .take(n_threads)
        .collect::<Vec<_>>();
    let stealers = workers.iter()
        .map(|w| w.stealer())
        .collect::<Vec<_>>();
    let _threads = workers.into_iter().map(|local| {
        let global = global.clone();
        let stealers = stealers.clone();
        let url_counter = url_counter.clone();
        let seen = seen.clone();
        thread::spawn(move || crawler(local, global, stealers, seen, url_counter))
    }).collect::<Vec<_>>();

    let mut old_count = url_counter.load(Ordering::Relaxed);
    loop {
        thread::sleep(Duration::from_millis(1000));
        let new_count = url_counter.load(Ordering::Relaxed);
        println!("{} urls/s", new_count - old_count);
        old_count = new_count;
    }
}
