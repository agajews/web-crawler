use web_index::IndexShard;
use std::path::PathBuf;
use std::env;
use std::collections::BinaryHeap;

#[derive(Eq, PartialEq)]
struct QueryMatch {
    id: usize,
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
    let query = "admissions";

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");
    let idxs = IndexShard::find_idxs(&index_dir);
    let (core, idx) = &idxs[0];
    let mut shard = IndexShard::open(&index_dir, &meta_dir, core, *idx).unwrap();
    println!("num terms: {}", shard.num_terms());
    let postings = shard.get_postings(query).unwrap();
    println!("opened postings");
    let meta = shard.open_meta();
    println!("opened meta");
    let mut heap = BinaryHeap::new();
    for (id, val) in postings.iter().take(20).enumerate() {
        heap.push(QueryMatch { id: id, val: *val });
    }
    for (id, val) in postings.iter().enumerate().skip(20) {
        if *val > heap.peek().unwrap().val {
            heap.pop();
            heap.push(QueryMatch { id: id, val: *val });
        }
    }
    let results = heap.into_vec();
    for result in results {
        println!("got url {}: {}, {}", meta[result.id].url, result.val, shard.term_counts()[result.id]);
    }
}
