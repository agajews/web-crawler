use web_index::IndexShard;
use std::path::PathBuf;
use std::env;
use std::collections::BinaryHeap;
use std::time::Instant;

#[derive(Eq, PartialEq)]
struct QueryMatch {
    id: usize,
    shard_id: usize,
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


fn main() {
    let query = "robotics";

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");
    let idxs = IndexShard::find_idxs(&index_dir);
    let mut shards = Vec::new();
    for (core, idx) in idxs {
        println!("opening shard {}:{}", core, idx);
        if let Some(shard) = IndexShard::open(&index_dir, &meta_dir, &core, idx) {
            shards.push(shard);
        }
    }
    println!("finished opening {} shards", shards.len());

    let mut heap = BinaryHeap::new();
    for i in 0..20 {
        heap.push(QueryMatch { id: i, shard_id: 0, val: 0 });
    }
    let start = Instant::now();
    for (shard_id, shard) in shards.iter_mut().enumerate() {
        if let Some(postings) = shard.get_postings(query) {
            let postings = postings.decode(100000);
            for (id, val) in postings.into_iter().enumerate() {
                if val > heap.peek().unwrap().val {
                    heap.pop();
                    heap.push(QueryMatch { id, shard_id, val });
                }
            }
        }
    }
    println!("time to search: {:?}", start.elapsed());
    let results = heap.into_vec();
    for result in results {
        let shard = &shards[result.shard_id];
        let meta = shard.open_meta();
        println!("got url {}: {}, {}", meta[result.id].url, result.val, shard.term_counts()[result.id]);
    }
}
