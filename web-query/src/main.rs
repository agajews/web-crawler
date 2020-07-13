use web_index::{IndexShard, IndexHeader, RleEncoding};
use std::path::{Path, PathBuf};
use std::env;
// use std::collections::BinaryHeap;
use std::time::Instant;
use std::fs;
use bincode::{deserialize, serialize};

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


fn fill_cache(index_dir: &Path, meta_dir: &Path, cache_path: &Path) -> Vec<IndexHeader> {
    let idxs = IndexShard::find_idxs(&index_dir);
    let mut shards = Vec::with_capacity(idxs.len());
    for (core, idx) in idxs {
        println!("opening shard {}:{}", core, idx);
        if let Some(shard) = IndexShard::open(&index_dir, &meta_dir, core, idx) {
            shards.push(shard);
        }
    }
    let headers = shards.into_iter().map(IndexShard::into_header).collect::<Vec<_>>();
    let bytes = serialize(&headers).unwrap();
    fs::write(cache_path, bytes).unwrap();
    headers
}


fn main() {
    let query = "robotics";

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");

    let cache_path: PathBuf = env::var("CACHE_PATH").unwrap().into();
    let post_dir: PathBuf = env::var("POST_DIR").unwrap().into();
    let bincode_dir = post_dir.join("bincode");
    let custom_dir = post_dir.join("custom");

    println!("trying to read from cache...");
    let headers = match fs::read(&cache_path) {
        Ok(bytes) => deserialize(&bytes).unwrap(),
        Err(_) => fill_cache(&index_dir, &meta_dir, &cache_path),
    };
    println!("finished reading and deserializing");

    let mut shards = headers.into_iter()
        .map(|header| IndexShard::from_header(header, &index_dir, &meta_dir))
        .collect::<Vec<_>>();
    println!("finished opening {} shards", shards.len());

    for (shard_id, shard) in shards.iter_mut().enumerate() {
        let postings = match shard.get_postings(query) {
            Some(postings) => postings,
            None => continue,
        };
        fs::write(bincode_dir.join(format!("{}", shard_id)), serialize(&postings).unwrap()).unwrap();
        fs::write(custom_dir.join(format!("{}", shard_id)), postings.serialize()).unwrap();
    }

    let start = Instant::now();
    for i in 0..100 {
        println!("benchmarking iter {}, time {:?}", i, start.elapsed());
        for entry in fs::read_dir(&bincode_dir).unwrap() {
            let path = entry.unwrap().path();
            let bytes = fs::read(path).unwrap();
            let _data: RleEncoding = deserialize(&bytes).unwrap();
        }
    }
    let bincode_time = start.elapsed() / 100;
    println!("time to load bincode: {:?}", bincode_time);

    let start = Instant::now();
    for i in 0..100 {
        println!("benchmarking iter {}, time {:?}", i, start.elapsed());
        for entry in fs::read_dir(&custom_dir).unwrap() {
            let path = entry.unwrap().path();
            let bytes = fs::read(path).unwrap();
            let _data = RleEncoding::deserialize(bytes).unwrap();
        }
    }
    let custom_time = start.elapsed() / 100;
    println!("time to load custom: {:?}", custom_time);
    println!("time to load bincode: {:?}", bincode_time);

    // let mut heap = BinaryHeap::new();
    // for i in 0..20 {
    //     heap.push(QueryMatch { id: i, shard_id: 0, val: 0 });
    // }

    // let start = Instant::now();
    // for i in 0..100 {
    //     println!("searching iter {}, time {:?}", i, start.elapsed());
    //     for (shard_id, shard) in shards.iter_mut().enumerate() {
    //         let postings = match shard.get_postings(query) {
    //             Some(postings) => postings,
    //             None => continue,
    //         };
    //         let postings = postings.decode(100000);
    //         for (id, val) in postings.into_iter().enumerate() {
    //             if val > heap.peek().unwrap().val {
    //                 heap.pop();
    //                 heap.push(QueryMatch { id, shard_id, val });
    //             }
    //         }
    //     }
    // }
    // println!("time to search bincode: {:?}", start.elapsed() / 100);

    // let results = heap.into_vec();
    // for result in results {
    //     let shard = &shards[result.shard_id];
    //     let meta = shard.open_meta();
    //     println!("got url {}: {}, {}", meta[result.id].url, result.val, shard.term_counts()[result.id]);
    // }
}
