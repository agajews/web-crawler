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
        simd.write_to_slice_unaligned(&mut posting[i..]);
    }
}

fn join_scores(scores: &mut [u8], posting: &[u8]) {
    let zero = u8x32::splat(0);
    for i in (0..posting.len()).step_by(32) {
        let mut score_simd = u8x32::from_slice_unaligned(&scores[i..]);
        let posting_simd = u8x32::from_slice_unaligned(&posting[i..]);
        score_simd *= u8x32::from_cast(posting_simd.ne(zero));
        score_simd += u8x32::from_cast(score_simd.ne(zero)) * posting_simd;
        score_simd.write_to_slice_unaligned(&mut scores[i..]);
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

fn get_scores(shard: &mut IndexShard, terms: &[String]) -> Option<Vec<u8>> {
    let mut postings = Vec::with_capacity(terms.len());
    for term in terms {
        postings.push(shard.get_postings(term, SHARD_SIZE)?);
    }
    let mut idfs = compute_idfs(&postings);
    // println!("idfs: {:?}", idfs);
    let min_idf = *idfs.iter().min().unwrap();
    for i in 0..idfs.len() {
        idfs[i] /= min_idf;
    }
    for i in 0..postings.len() {
        divide_scores(&mut postings[i], idfs[i]);
    }
    let maxs = postings.iter()
        .map(|posting| compute_max(&posting) as u32)
        .collect::<Vec<_>>();
    let total_max = maxs.iter().sum::<u32>();
    let denominator = (total_max / 255 + 1) as u8;
    for i in 0..postings.len() {
        divide_scores(&mut postings[i], denominator);
    }
    let mut scores = postings.pop().unwrap();
    for posting in postings {
        join_scores(&mut scores, &posting);
    }
    Some(scores)
}

// fn get_scores(shard: &mut IndexShard, terms: &[String]) -> Option<Vec<u8>> {
//     let mut postings = Vec::with_capacity(terms.len());
//     for term in terms {
//         postings.push(shard.get_postings(term, SHARD_SIZE)?);
//     }
//     let mut idfs = postings.iter()
//         .map(|posting| (posting.iter().filter(|byte| **byte > 0).count() * (1 << 15)) / SHARD_SIZE)
//         .map(|idf| 32 - (idf as u32).leading_zeros())
//         .map(|log_idf| 1 << (log_idf / 2))
//         .collect::<Vec<_>>();
//     let min_idf = *idfs.iter().min().unwrap();
//     // println!("idfs: {:?}", idfs);
//     for i in 0..idfs.len() {
//         idfs[i] /= min_idf;
//     }
//     for i in 0..postings.len() {
//         let posting = &mut postings[i];
//         let idf = idfs[i];
//         for j in 0..posting.len() {
//             posting[j] /= idf;
//         }
//     }
//     let maxs = postings.iter()
//         .map(|posting| *posting.iter().max().unwrap() as u32)
//         .collect::<Vec<_>>();
//     let total_max = maxs.iter().sum::<u32>();
//     let denominator = (total_max / 255 + 1) as u8;
//     let mut scores = postings.pop().unwrap();
//     for j in 0..scores.len() {
//         scores[j] /= denominator;
//     }
//     for posting in postings {
//         for j in 0..posting.len() {
//             if scores[j] > 0 {
//                 if posting[j] > 0 {
//                     scores[j] += posting[j] / denominator;
//                 } else {
//                     scores[j] = 0;
//                 }
//             }
//         }
//     }
//     Some(scores)
// }

fn main() {
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

    // let mut count = 0;
    // let start = Instant::now();
    // for _ in 0..10 {
    //     for shard in shards.iter_mut() {
    //         if let Some(postings) = shard.get_postings(&terms[0], SHARD_SIZE) {
    //             count += postings.len();
    //         }
    //     }
    // }
    // println!("time to search: {:?}", start.elapsed() / 10);
    // println!("count: {}", count);

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

    let results = heap.into_sorted_vec();
    for result in results {
        let shard = &mut shards[result.shard_id];
        let url = shard.get_url(result.id).unwrap();
        println!("got url {}: {}, {}", url, result.val, shard.term_counts()[result.id]);
    }
}
