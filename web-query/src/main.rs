use web_index::IndexShard;
use std::path::PathBuf;
use std::env;
use std::collections::BinaryHeap;
use std::time::Instant;
use packed_simd::*;
use std::fs;
use tokio::sync::mpsc::channel;

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

fn join_scores(scores: &[u8], postings: Vec<Vec<u8>>, mut denominators: Vec<u32>, term_counts: &[u8], shard_id: usize, heap: &mut BinaryHeap<QueryMatch>) {
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
                    heap.push(QueryMatch { id, shard_id, val });
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

fn add_scores(mut postings: Vec<Vec<u8>>, term_counts: &[u8], heap: &mut BinaryHeap<QueryMatch>, shard_id: usize) {
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
    join_scores(&scores, postings, denominators, term_counts, shard_id, heap);
}

#[tokio::main]
async fn main() {
    let terms = env::args().skip(1).collect::<Vec<_>>();

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");

    let post_dir: PathBuf = env::var("POST_DIR").unwrap().into();
    fs::create_dir_all(&post_dir).unwrap();

    let idxs = IndexShard::find_idxs(&index_dir);
    println!("found {} idxs", idxs.len());
    let mut shards = Vec::with_capacity(idxs.len());
    for (core, idx) in idxs {
        if idx == 0 {
            println!("opening shard {}:{}", core, idx);
        }
        if let Some(shard) = IndexShard::open(&index_dir, &meta_dir, core, idx).await {
            shards.push(Some(shard));
        }
    }
    println!("finished opening {} shards", shards.len());

    let n_shards = shards.len();

    for _ in 0..20 {
        let start = Instant::now();
        let mut heap = BinaryHeap::new();
        for i in 0..20 {
            heap.push(QueryMatch { id: i, shard_id: 0, val: 0 });
        }

        let (sender, mut receiver) = channel(n_shards);

        for (shard_id, shard) in shards.drain(0..).enumerate() {
            let terms = terms.clone();
            let mut sender = sender.clone();
            tokio::spawn(async move {
                println!("spawning shard {}", shard_id);
                let mut postings = Vec::new();
                let mut shard = shard.unwrap();
                for term in terms {
                    postings.push(shard.get_postings(&term, SHARD_SIZE).await);
                }
                let postings = postings.into_iter().collect::<Option<Vec<_>>>();
                let _res = sender.send((shard_id, shard, postings)).await;
            });
        }

        for _ in 0..n_shards {
            shards.push(None);
        }

        let mut count = 0;
        while let Some((shard_id, shard, postings)) = receiver.recv().await {
            println!("read postings for shard {}", shard_id);
            if let Some(postings) = postings {
                let term_counts = shard.term_counts();
                add_scores(postings, term_counts, &mut heap, shard_id);
                shards[shard_id] = Some(shard);
                count += 1;
            }
        }

        println!("time to search: {:?}", start.elapsed());
        println!("found postings in {} shards", count);
    }

    // let results = heap.into_sorted_vec();
    // for result in results {
    //     let mut shard = shards[result.shard_id].lock().await;
    //     let url = shard.get_url(result.id).unwrap();
    //     println!("got url {}: {}, {}", url, result.val, shard.term_counts()[result.id]);
    // }

    // let start = Instant::now();
    // for _ in 0..20 {
    //     let mut heap = BinaryHeap::new();
    //     for i in 0..20 {
    //         heap.push(QueryMatch { id: i, shard_id: 0, val: 0 });
    //     }
    //     for (shard_id, shard) in shards.iter_mut().enumerate() {
    //         let postings = terms.iter()
    //             .map(|term| shard.get_postings(term, SHARD_SIZE))
    //             .collect::<Option<Vec<_>>>();
    //         if let Some(postings) = postings {
    //             let term_counts = shard.term_counts();
    //             add_scores(postings, term_counts, &mut heap, shard_id);
    //         }
    //     }
    // }
    // println!("time to search: {:?}", start.elapsed() / 20);
}
