use web_index::IndexShard;
use std::path::PathBuf;
use std::env;

fn main() {
    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");
    let shards = IndexShard::open_all(index_dir, meta_dir);

    let postings = shards[0].get_postings("robotics");
    println!("{:?}", postings);
}
