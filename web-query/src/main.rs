use web_index::IndexShard;
use std::path::PathBuf;
use std::env;
use std::collections::BinaryHeap;
use std::time::Instant;
use std::sync::{Arc, Mutex};

const N_THREADS: usize = 4;

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


#[tokio::main]
async fn main() {
    let query = "robotics";

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");
    let idxs = Arc::new(IndexShard::find_idxs(&index_dir));

    let mut threads = Vec::with_capacity(N_THREADS);
    for tid in 0..N_THREADS {
        let idxs = idxs.clone();
        let index_dir = index_dir.clone();
        let meta_dir = meta_dir.clone();
        threads.push(tokio::spawn(async move {
            let shards_per_thread = (idxs.len() + N_THREADS - 1) / N_THREADS;
            let start = shards_per_thread * tid;
            let end = std::cmp::min(shards_per_thread * (tid + 1), idxs.len());
            let mut shards = Vec::with_capacity(end - start);
            for shard_id in start..end {
                let (core, idx) = &idxs[shard_id];
                println!("opening shard {}:{}", core, idx);
                let shard = match IndexShard::open(&index_dir, &meta_dir, core, *idx).await {
                    Some(shard) => shard,
                    None => continue,
                };
                shards.push(shard);
            }
            shards
        }));
    }
    let mut shard_vecs = Vec::with_capacity(N_THREADS);
    for thread in threads {
        shard_vecs.push(thread.await.unwrap());
    }
    println!("finished opening {} shards", idxs.len());

    let mut heap = BinaryHeap::new();
    for i in 0..20 {
        heap.push(QueryMatch { id: i, tid: 0, shard_id: 0, val: 0 });
    }
    let heap = Arc::new(Mutex::new(heap));

    let mut threads = Vec::with_capacity(N_THREADS);
    let start = Instant::now();
    for (tid, mut shards) in shard_vecs.into_iter().enumerate() {
        let heap = heap.clone();
        threads.push(tokio::spawn(async move {
            for (shard_id, shard) in shards.iter_mut().enumerate() {
                let postings = match shard.get_postings(query).await {
                    Some(postings) => postings,
                    None => continue,
                };
                let mut heap = heap.lock().unwrap();
                let postings = postings.decode(100000);
                for (id, val) in postings.into_iter().enumerate() {
                    if val > heap.peek().unwrap().val {
                        heap.pop();
                        heap.push(QueryMatch { id, tid, shard_id, val });
                    }
                }
            }
            shards
        }));
    }
    let mut shard_vecs = Vec::with_capacity(N_THREADS);
    for thread in threads {
        shard_vecs.push(thread.await.unwrap());
    }
    println!("time to search: {:?}", start.elapsed());

    let heap = Arc::try_unwrap(heap).unwrap_or(Mutex::new(BinaryHeap::new())).into_inner().unwrap();
    let results = heap.into_vec();
    for result in results {
        let shard = &shard_vecs[result.tid][result.shard_id];
        let meta = shard.open_meta().await;
        println!("got url {}: {}, {}", meta[result.id].url, result.val, shard.term_counts()[result.id]);
    }
}
