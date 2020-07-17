use web_index::{IndexShard, DiskMeta, DiskMultiMap, RunEncoder};
use std::path::PathBuf;
use std::env;
use std::collections::BTreeSet;
use std::thread;
use std::fs;

const SHARD_SIZE: usize = 100000;


fn shard_thread(tid: usize, chunk: Vec<(String, usize)>, index_dir: PathBuf, meta_dir: PathBuf, merged_dir: PathBuf) {
    let mut shards = Vec::with_capacity(chunk.len());
    for (core, idx) in chunk {
        if idx == 0 {
            println!("opened shard {}:{}", core, idx);
        }
        if let Some(shard) = IndexShard::open(&index_dir, &meta_dir, core, idx) {
            shards.push(shard);
        }
    }

    let mut keys = BTreeSet::new();
    for shard in &shards {
        for key in shard.keys() {
            keys.insert(key);
        }
    }
    let keys = keys.into_iter().collect::<Vec<_>>();
    println!("found {} keys", keys.len());

    let mut merged = Vec::with_capacity(keys.len());
    let mut term_counts = Vec::with_capacity(SHARD_SIZE * shards.len());
    let mut urls = Vec::with_capacity(SHARD_SIZE * shards.len());
    for (key_count, key) in keys.into_iter().enumerate() {
        println!("thread {} on key {}", tid, key_count);
        let mut encoder = RunEncoder::new();
        let mut i = 0;
        for shard in &mut shards {
            if let Some(postings) = shard.get_postings(key, SHARD_SIZE) {
                for byte in postings {
                    encoder.add(i, byte);
                    i += 1;
                }
            } else {
                for _ in 0..SHARD_SIZE {
                    encoder.add(i, 0);
                    i += 1;
                }
            }
            term_counts.extend_from_slice(shard.term_counts());
            for i in 0..SHARD_SIZE {
                urls.push(shard.get_url(i).unwrap());
            }
        }
        merged.push((key, encoder));
    }

    let (metadata, data) = DiskMultiMap::serialize_cache(merged);
    let url_bytes = DiskMeta::serialize_urls(urls);

    let path = merged_dir.join(format!("shard{}", tid));
    fs::create_dir_all(&path).unwrap();
    fs::write(&path.join("metadata"), &metadata).unwrap();
    fs::write(&path.join("index"), &data).unwrap();
    fs::write(&path.join("urls"), &url_bytes).unwrap();
    fs::write(&path.join("terms"), &term_counts).unwrap();
}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let n_threads = args[0].parse::<usize>().unwrap();

    let top_dir: PathBuf = env::var("CRAWLER_DIR").unwrap().into();
    let index_dir = top_dir.join("index");
    let meta_dir = top_dir.join("meta");
    let merged_dir: PathBuf = env::var("MERGED_DIR").unwrap().into();

    let idxs = IndexShard::find_idxs(&index_dir);
    println!("found {} idxs", idxs.len());
    let shards_per_thread = (idxs.len() + n_threads - 1) / n_threads;
    let chunks = idxs.chunks(shards_per_thread);
    let mut threads = Vec::new();
    for (tid, chunk) in chunks.into_iter().enumerate() {
        let chunk = chunk.to_vec();
        let index_dir = index_dir.clone();
        let meta_dir = meta_dir.clone();
        let merged_dir = merged_dir.clone();
        threads.push(thread::spawn(move || shard_thread(tid, chunk, index_dir, meta_dir, merged_dir) ));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
