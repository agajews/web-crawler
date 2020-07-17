#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use] extern crate rocket;

use rocket::State;
use web_index::IndexShard;
use std::path::PathBuf;
use std::env;
use std::collections::BinaryHeap;
use std::time::Instant;
use packed_simd::*;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use rocket_contrib::templates::Template;
use serde::Serialize;

const SHARD_SIZE: usize = 100000;

#[derive(Eq, PartialEq)]
struct QueryMatch {
    id: usize,
    shard_id: usize,
    tid: usize,
    val: u8,
}

impl Ord for QueryMatch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // reversed for the min heap
        other.val.cmp(&self.val)
    }
}

impl PartialOrd for QueryMatch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}


fn count_nonzero(slice: &[u8]) -> u32 {
    // slice.iter().filter(|byte| **byte > 0).count() as u32
    let zero = u8x32::splat(0);
    let mut count = u8x32::splat(0);
    for i in (0..(slice.len())).step_by(32) {
        let simd = u8x32::from_slice_unaligned(&slice[i..]);
        count += u8x32::from_cast(simd.ne(zero)) >> 7;
    }
    let mut total_count: u32 = 0;
    for i in 0..32 {
        total_count += count.extract(i) as u32;
    }
    total_count
}


fn compute_idfs(postings: &Vec<Vec<u8>>) -> Vec<u32> {
    let mut idfs = Vec::with_capacity(postings.len());
    for posting in postings {
        let mut total_count: u32 = 0;
        for i in (0..(posting.len())).step_by(32 * 128) {
            let end = std::cmp::min(i + 32 * 128, posting.len());
            total_count += count_nonzero(&posting[i..end]);
        }
        let normalized_count = total_count * (1 << 16) / SHARD_SIZE as u32;
        let log_idf = 32 - normalized_count.leading_zeros();
        let idf = 1 << (log_idf / 2);
        idfs.push(idf);
    }
    idfs
}

fn join_scores(scores: &[u8], postings: Vec<Vec<u8>>, mut denominators: Vec<u32>, term_counts: &[u8], tid: usize, shard_id: usize, heap: &mut BinaryHeap<QueryMatch>) {
    let zero = u8x32::splat(0);
    for i in 0..denominators.len() {
        denominators[i] = 32 - denominators[i].next_power_of_two().leading_zeros() - 1;
    }
    let score_denominator = denominators.pop().unwrap();
    for i in (0..scores.len()).step_by(32) {
        let mut score_simd = u8x32::from_slice_unaligned(&scores[i..]);
        score_simd = score_simd >> score_denominator;
        for (posting, denominator) in postings.iter().zip(denominators.iter()) {
            let mut posting_simd = u8x32::from_slice_unaligned(&posting[i..]);
            posting_simd = posting_simd >> *denominator;
            score_simd &= u8x32::from_cast(posting_simd.ne(zero));
            score_simd += u8x32::from_cast(score_simd.ne(zero)) & posting_simd;
        }
        // score_simd.write_to_slice_unaligned(&mut scores[i..]);

        let prev_val = heap.peek().unwrap().val;
        if score_simd.gt(u8x32::splat(prev_val)).any() {
            for j in 0..32 {
                let val = score_simd.extract(j);
                let id = i + j;
                if val > heap.peek().unwrap().val && term_counts[id] >= 8 {
                    heap.pop();
                    heap.push(QueryMatch { id, tid, shard_id, val });
                }
            }
        }
    }
}

fn compute_max(scores: &[u8]) -> u8 {
    let mut maxs = u8x32::splat(0);
    for i in (0..scores.len()).step_by(32) {
        let simd = u8x32::from_slice_unaligned(&scores[i..]);
        maxs = maxs.max(simd);
    }
    let mut max = 0;
    for i in 0..32 {
        max = std::cmp::max(max, maxs.extract(i));
    }
    max
}

fn add_scores(mut postings: Vec<Vec<u8>>, term_counts: &[u8], heap: &mut BinaryHeap<QueryMatch>, tid: usize, shard_id: usize) {
    let mut idfs = compute_idfs(&postings);
    // println!("idfs: {:?}", idfs);
    let min_idf = *idfs.iter().min().unwrap();
    for i in 0..idfs.len() {
        idfs[i] /= min_idf;
    }
    let maxs = postings.iter()
        .zip(idfs.iter())
        .map(|(posting, idf)| (compute_max(&posting) as u32 / idf) as u32)
        .collect::<Vec<_>>();
    let total_max = maxs.iter().sum::<u32>();
    let denominator = total_max / 255 + 1;
    let denominators = idfs.into_iter()
        .map(|idf| idf * denominator)
        .collect::<Vec<_>>();
    let scores = postings.pop().unwrap();
    join_scores(&scores, postings, denominators, term_counts, tid, shard_id, heap);
}

#[derive(Clone)]
struct QueryJob {
    terms: Vec<String>,
    heap: Arc<Mutex<BinaryHeap<QueryMatch>>>,
    done_sender: Sender<usize>,
}

struct UrlJob {
    shard_id: usize,
    id: usize,
    done_sender: Sender<(String, u8)>,
}

enum Job {
    Query(QueryJob),
    Url(UrlJob),
}

fn handle_query(shards: &mut [IndexShard], job: QueryJob, tid: usize) {
    let mut count = 0;
    for (shard_id, shard) in shards.iter_mut().enumerate() {
        let mut postings = Vec::new();
        for term in &job.terms {
            postings.push(shard.get_postings(term, SHARD_SIZE));
        }
        let postings = postings.into_iter().collect::<Option<Vec<_>>>();
        if let Some(postings) = postings {
            let term_counts = shard.term_counts();
            let mut heap = job.heap.lock().unwrap();
            add_scores(postings, term_counts, &mut heap, tid, shard_id);
            count += 1;
        }
    }
    job.done_sender.send(count).unwrap();
}

fn handle_url(shards: &mut [IndexShard], job: UrlJob) {
    let shard = &mut shards[job.shard_id];
    let url = shard.get_url(job.id).unwrap();
    let term_count = shard.term_counts()[job.id];
    job.done_sender.send((url, term_count)).unwrap();
}

fn shard_thread(tid: usize, chunk: Vec<(String, usize)>, index_dir: PathBuf, meta_dir: PathBuf, ready_sender: Sender<()>, work_receiver: Receiver<Job>) {
    let mut shards = Vec::with_capacity(chunk.len());
    for (core, idx) in chunk {
        if idx == 0 {
            println!("opened shard {}:{}", core, idx);
        }
        if let Some(shard) = IndexShard::open(&index_dir, &meta_dir, core, idx) {
            shards.push(shard);
        }
    }
    ready_sender.send(()).unwrap();

    for job in work_receiver {
        match job {
            Job::Query(job) => handle_query(&mut shards, job, tid),
            Job::Url(job) => handle_url(&mut shards, job),
        }
    }
}

fn spawn_threads(n_threads: usize) -> Vec<Sender<Job>> {
    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");

    let idxs = IndexShard::find_idxs(&index_dir);
    println!("found {} idxs", idxs.len());
    let shards_per_thread = (idxs.len() + n_threads - 1) / n_threads;
    let chunks = idxs.chunks(shards_per_thread);
    let (ready_sender, ready_receiver) = channel();
    let mut work_senders = Vec::new();
    for (tid, chunk) in chunks.into_iter().enumerate() {
        let (work_sender, work_receiver) = channel();
        work_senders.push(work_sender);
        let chunk = chunk.to_vec();
        let index_dir = index_dir.clone();
        let meta_dir = meta_dir.clone();
        let ready_sender = ready_sender.clone();
        thread::spawn(move || shard_thread(tid, chunk, index_dir, meta_dir, ready_sender, work_receiver) );
    }

    for _ in 0..n_threads {
        ready_receiver.recv().unwrap();
    }

    work_senders
}

#[derive(Serialize)]
struct SearchResult {
    url: String,
    score: u8,
    term_count: u8,
}

fn compute_search(work_senders: State<Arc<Mutex<Vec<Sender<Job>>>>>, terms: Vec<String>) -> (usize, Vec<SearchResult>) {
    let mut heap = BinaryHeap::new();
    for i in 0..20 {
        heap.push(QueryMatch { id: i, shard_id: 0, val: 0, tid: 0 });
    }
    let heap = Arc::new(Mutex::new(heap));

    let (done_sender, done_receiver) = channel();

    let job = QueryJob {
        done_sender,
        terms: terms.clone(),
        heap: heap.clone(),
    };

    let n_threads = {
        let work_senders = work_senders.lock().unwrap();
        for work_sender in work_senders.iter() {
            let job = job.clone();
            work_sender.send(Job::Query(job)).unwrap();
        }
        work_senders.len()
    };

    let mut count = 0;
    for _ in 0..n_threads {
        count += SHARD_SIZE * done_receiver.recv().unwrap();
    }

    let mut results = heap.lock().unwrap().drain().collect::<Vec<_>>();
    results.sort();
    let mut done_receivers = Vec::with_capacity(n_threads);
    {
        let work_senders = work_senders.lock().unwrap();
        for result in &results {
            let (done_sender, done_receiver) = channel();
            done_receivers.push(done_receiver);
            let job = UrlJob { shard_id: result.shard_id, id: result.id, done_sender };
            work_senders[result.tid].send(Job::Url(job)).unwrap();
        }
    }

    let mut search_results = Vec::new();
    for (result, done_receiver) in results.iter().zip(done_receivers) {
        let (url, term_count) = done_receiver.recv().unwrap();
        search_results.push(SearchResult { url, term_count, score: result.val });
    }

    (count, search_results)
}

#[derive(Serialize)]
struct SearchContext {
    count: usize,
    results: Vec<SearchResult>,
    search_time: usize,
}

fn split_query(query: &str) -> Vec<String> {
    let terms = query.split_whitespace()
        .map(|term| term.to_lowercase())
        .collect::<Vec<String>>();
    terms
}

#[get("/search?<query>")]
fn search(query: String, work_senders: State<Arc<Mutex<Vec<Sender<Job>>>>>) -> Template {
    let terms = split_query(&query);
    let start = Instant::now();
    let (count, results) = compute_search(work_senders, terms);
    let context = SearchContext { count, results, search_time: start.elapsed().as_millis() as usize };
    Template::render("search", &context)
}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let n_threads = args[0].parse::<usize>().unwrap();

    let work_senders = Arc::new(Mutex::new(spawn_threads(n_threads)));

    rocket::ignite()
        .attach(Template::fairing())
        .manage(work_senders)
        .mount("/", routes![search])
        .launch();
}
