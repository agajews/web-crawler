use web_index::IndexShard;
use std::path::PathBuf;
use std::env;


fn main() {
    let query = "robotics";

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");
    let idxs = IndexShard::find_idxs(&index_dir);
    // let mut shards = Vec::new();
    // for (core, idx) in idxs {
    //     println!("opening {}: {}", core, idx);
    //     if let Some(shard) = IndexShard::open(&index_dir, &meta_dir, &core, idx) {
    //         shards.push(shard);
    //     }
    // }
    // println!("opened all shards!");
    let (core, idx) = &idxs[0];
    let mut shard = IndexShard::open(&index_dir, &meta_dir, core, *idx).unwrap();
    let postings = shard.get_postings(query).unwrap();
    let meta = shard.open_meta();
    for (k, bitset) in postings.iter().enumerate() {
        let mut bitset = *bitset;
        while bitset != 0 {
          let t = bitset & bitset.wrapping_neg();
          let r = bitset.trailing_zeros();
          let url_id = (k as u32) * 8 + r;
          let url_meta = &meta[&url_id];
          println!("got url {}: {}", url_meta.url, url_meta.term_counts[query]);
          bitset ^= t;
        }
    }
    // let mut top_matches = Vec::new();
    // for (shard_id, shard) in shards.iter_mut().enumerate() {
    //     let postings = shard.get_postings(query);
    //     let term_counts = &shard.term_counts;
    //     let mut matches = postings.into_iter()
    //         .map(|posting| {
    //             let n_terms = term_counts[&posting.url_id] as f32;
    //             let count = posting.count as f32;
    //             let q = count / n_terms;
    //             (posting, shard_id, q)
    //         })
    //         .collect::<Vec<_>>();
    //     matches.sort_by(|(_, _, q_a), (_, _, q_b)| q_b.partial_cmp(q_a).unwrap());
    //     for post in matches.into_iter().take(10) {
    //         top_matches.push(post);
    //     }
    // }
    // top_matches.sort_by(|(_, _, q_a), (_, _, q_b)| q_b.partial_cmp(q_a).unwrap());
    // let matches = &top_matches[0..10];

    // for (posting, shard_id, q) in matches {
    //     let meta = shards[*shard_id].open_meta();
    //     let url = &meta[&posting.url_id].url;
    //     println!("{}: {} - {}", url, q, posting.count);
    // }
}
