use web_index::IndexShard;
use std::path::PathBuf;
use std::env;
use std::collections::BinaryHeap;
use std::time::Instant;
use packed_simd::*;

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


fn count_nonzero(slice: &[u8]) -> u32 {
    let zero = u8x32::splat(0);
    let mut count = u8x32::splat(0);
    for i in (0..(slice.len())).step_by(32) {
        let simd = u8x32::from_slice_unaligned(&slice[i..]);
        count += u8x32::from_cast(simd.ne(zero));
    }
    let mut total_count: u32 = 0;
    for i in 0..32 {
        total_count += count.extract(i) as u32;
    }
    total_count
}


fn compute_idfs(postings: &Vec<Vec<u8>>) -> Vec<u8> {
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

fn divide_scores(posting: &mut [u8], denominator: u8) {
    for i in (0..posting.len()).step_by(32) {
        let mut simd = u8x32::from_slice_unaligned(&posting[i..]);
        simd /= denominator;
    }
}

fn join_scores(scores: &mut [u8], posting: &mut [u8]) {
    for j in 0..posting.len() {
        if scores[j] > 0 {
            if posting[j] > 0 {
                scores[j] += posting[j];
            } else {
                scores[j] = 0;
            }
        }
    }
}

fn get_scores(shard: &mut IndexShard, terms: &[String]) -> Option<Vec<u8>> {
    let mut postings = Vec::with_capacity(terms.len());
    for term in terms {
        postings.push(shard.get_postings(term, SHARD_SIZE)?);
    }
    let idfs = compute_idfs(&postings);
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
    let denominator = (total_max / 255 + 1) as u8;
    let mut scores = postings.pop().unwrap();
    for j in 0..scores.len() {
        scores[j] /= denominator;
    }
    for posting in postings {
        for j in 0..posting.len() {
            if scores[j] > 0 {
                if posting[j] > 0 {
                    scores[j] += posting[j] / denominator;
                } else {
                    scores[j] = 0;
                }
            }
        }
    }
    Some(scores)
}

fn main() {
    let query = "robotics";
    let query_b = "meta";
    // let terms = env::args().skip(1).collect::<Vec<_>>();

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
    for _ in 0..100 {
        for shard in &mut shards {
            let mut posting = shard.get_postings(query, SHARD_SIZE).unwrap();
            let mut posting_b = shard.get_postings(query_b, SHARD_SIZE).unwrap();
            join_scores(&mut posting, &mut posting_b);
        }
    }
    println!("time to compute: {:?}", start.elapsed() / 100);

    // for _ in 0..100 {
    //     let start = Instant::now();
    //     let mut heap = BinaryHeap::new();
    //     for i in 0..20 {
    //         heap.push(QueryMatch { id: i, shard_id: 0, val: 0 });
    //     }
    //     for (shard_id, shard) in shards.iter_mut().enumerate() {
    //         let postings = match get_scores(shard, &terms) {
    //             Some(postings) => postings,
    //             None => continue,
    //         };
    //         let term_counts = shard.term_counts();
    //         for (id, val) in postings.into_iter().enumerate() {
    //             if val > heap.peek().unwrap().val && term_counts[id] >= 8 {
    //                 heap.pop();
    //                 heap.push(QueryMatch { id, shard_id, val });
    //             }
    //         }
    //     }
    //     println!("time to search: {:?}", start.elapsed());
    // }

    // let results = heap.into_sorted_vec();
    // for result in results {
    //     let shard = &mut shards[result.shard_id];
    //     let url = shard.get_url(result.id).unwrap();
    //     println!("got url {}: {}, {}", url, result.val, shard.term_counts()[result.id]);
    // }
}
