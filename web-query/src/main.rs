use web_index::IndexShard;
use std::path::PathBuf;
use std::env;


fn main() {
    let query = "columbia";

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
    for (id, val) in postings.into_iter().enumerate() {
        if val > 0 {
            println!("got url {}: {}", meta[id].url, val);
        }
    }
}
