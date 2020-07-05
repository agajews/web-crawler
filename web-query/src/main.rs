use web_index::IndexShard;
use std::path::PathBuf;
use std::env;


fn main() {
    let query = "robotics";

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");
    let idxs = IndexShard::find_idxs(&index_dir);
    let mut shards = Vec::new();
    for (core, idx) in idxs {
        let shard = IndexShard::open(&index_dir, &meta_dir, &core, idx).unwrap();
        shards.push(shard);
    }
    println!("opened all shards!");
    let mut top_matches = Vec::new();
    for (shard_id, shard) in shards.iter_mut().enumerate() {
        let postings = shard.get_postings(query);
        let term_counts = &shard.term_counts;
        let mut matches = postings.into_iter()
            .map(|posting| (posting.url_id, shard_id, posting.count as f32 / term_counts[&posting.url_id] as f32))
            .collect::<Vec<_>>();
        matches.sort_by(|(_, _, q_a), (_, _, q_b)| q_b.partial_cmp(q_a).unwrap());
        for post in matches.into_iter().take(10) {
            top_matches.push(post);
        }
    }
    top_matches.sort_by(|(_, _, q_a), (_, _, q_b)| q_b.partial_cmp(q_a).unwrap());
    let matches = &top_matches[0..10];

    for (id, shard_id, q) in matches {
        let meta = shards[*shard_id].open_meta();
        let url = &meta[id].url;
        println!("{}: {}", url, q);
    }
}
