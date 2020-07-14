use web_index::IndexShard;
use std::path::PathBuf;
use std::env;
use std::collections::BinaryHeap;
use std::time::Instant;

const SHARD_SIZE: usize = 100000;

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


fn get_scores(shard: &mut IndexShard, terms: &[String]) -> Option<Vec<u8>> {
    let mut postings = Vec::with_capacity(terms.len());
    for term in terms {
        postings.push(shard.get_postings(term, SHARD_SIZE)?);
    }
    let mut idfs = postings.iter()
        .map(|posting| posting.iter().filter(|byte| **byte > 0).count() as u32)
        .map(|idf| 32 - idf.leading_zeros() as u8 + 1)
        .collect::<Vec<_>>();
    let min_idf = *idfs.iter().min().unwrap();
    println!("idfs: {:?}", idfs);
    for i in 0..idfs.len() {
        idfs[i] /= min_idf;
    }
    for i in 0..postings.len() {
        let posting = &mut postings[i];
        let idf = idfs[i];
        for j in 0..posting.len() {
            posting[j] /= idf;
        }
    }
    let maxs = postings.iter()
        .map(|posting| *posting.iter().max().unwrap() as u32)
        .collect::<Vec<_>>();
    let total_max = maxs.iter().sum::<u32>();
    let denominator = (total_max / 255) as u8;
    let mut scores = postings.pop().unwrap();
    for j in 0..scores.len() {
        scores[j] /= denominator;
    }
    for posting in postings {
        for j in 0..posting.len() {
            scores[j] += posting[j] / denominator;
        }
    }
    Some(scores)
}

fn main() {
    // let query = "robotics";
    let terms = env::args().skip(1).collect::<Vec<_>>();

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");

    let idxs = IndexShard::find_idxs(&index_dir);
    let mut shards = Vec::with_capacity(idxs.len());
    for (core, idx) in idxs {
        if idx == 0 {
            println!("opening shard {}:{}", core, idx);
        }
        if let Some(shard) = IndexShard::open(&index_dir, &meta_dir, core, idx) {
            shards.push(shard);
        }
    }
    println!("finished opening {} shards", shards.len());

    let start = Instant::now();
    let mut heap = BinaryHeap::new();
    for i in 0..20 {
        heap.push(QueryMatch { id: i, shard_id: 0, val: 0 });
    }
    for (shard_id, shard) in shards.iter_mut().enumerate() {
        let postings = match get_scores(shard, &terms) {
            Some(postings) => postings,
            None => continue,
        };
        let term_counts = shard.term_counts();
        for (id, val) in postings.into_iter().enumerate() {
            if val > heap.peek().unwrap().val && term_counts[id] >= 8 {
                heap.pop();
                heap.push(QueryMatch { id, shard_id, val });
            }
        }
    }
    println!("time to search: {:?}", start.elapsed());

    let results = heap.into_vec();
    for result in results {
        let shard = &mut shards[result.shard_id];
        let url = shard.get_url(result.id).unwrap();
        println!("got url {}: {}, {}", url, result.val, shard.term_counts()[result.id]);
    }
}
