use bincode::{serialize, deserialize};
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::fs;
use std::thread;
use rand;
use rand::Rng;
use std::collections::{VecDeque, BTreeMap};
use tokio::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::convert::TryInto;

#[derive(Serialize, Deserialize, Debug)]
enum RunSegment {
    Run(u32, u8),
    NonRun(Vec<u8>),
}

pub struct RunEncoder {
    len: usize,
    encoded: Vec<RunSegment>,
    non_run: Vec<u8>,
    run: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RleEncoding {
    len: usize,
    encoded: Vec<RunSegment>,
}

impl RunEncoder {
    pub fn new() -> RunEncoder {
        RunEncoder {
            encoded: Vec::new(),
            non_run: Vec::new(),
            run: Vec::new(),
            len: 0,
        }
    }

    pub fn add(&mut self, idx: usize, byte: u8) {
        assert!(idx >= self.len);
        if idx - self.len < 3 {
            for _ in 0..(idx - self.len) {
                self.push(0);
            }
            self.push(byte);
        } else {
            self.flush();
            self.encoded.push(RunSegment::Run((idx - self.len) as u32, 0));
            self.run.push(byte);
        }
        self.len = idx + 1;
    }

    fn push(&mut self, byte: u8) {
        match self.run.last() {
            Some(prev) if *prev == byte => self.run.push(byte),
            None => self.run.push(byte),
            _ => {
                if self.run.len() < 3 {
                    self.non_run.append(&mut self.run);
                } else {
                    self.sync_segs();
                }
                self.run.push(byte);
            }
        }
    }

    fn flush(&mut self) {
        if self.run.len() < 3 {
            self.non_run.append(&mut self.run);
        }
        self.sync_segs();
    }

    fn sync_segs(&mut self) {
        if self.non_run.len() > 0 {
            self.encoded.push(RunSegment::NonRun(self.non_run.clone()));
            self.non_run.clear();
        }
        if self.run.len() > 0 {
            self.encoded.push(RunSegment::Run(self.run.len() as u32, self.run[0]));
            self.run.clear();
        }
    }

    pub fn encode(mut self) -> RleEncoding {
        self.flush();
        RleEncoding { encoded: self.encoded, len: self.len }
    }
}

impl RleEncoding {
    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&(self.len as u32).to_be_bytes());
        serialized.extend_from_slice(&(self.encoded.len() as u32).to_be_bytes());
        for seg in &self.encoded {
            match seg {
                RunSegment::NonRun(bytes) => {
                    serialized.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                    serialized.extend_from_slice(&bytes);
                },
                RunSegment::Run(len, val) => {
                    serialized.extend_from_slice(&(len + (1 << 31)).to_be_bytes());
                    serialized.extend_from_slice(&val.to_be_bytes());
                }
            }
        }
        serialized
    }

    pub fn deserialize(serialized: Vec<u8>) -> Option<RleEncoding> {
        let len = u32::from_be_bytes(serialized[0..4].try_into().ok()?) as usize;
        let n_segs = u32::from_be_bytes(serialized[4..8].try_into().ok()?);
        let mut encoded = Vec::with_capacity(n_segs as usize);
        let mut i = 8;
        for j in 0..n_segs {
            // println!("at segment {}/{}", j, n_segs);
            let seg_len = u32::from_be_bytes(serialized[i..(i + 4)].try_into().ok()?);
            // println!("seg len: {}", seg_len);
            i += 4;
            if seg_len >= (1 << 31) {
                encoded.push(RunSegment::Run(seg_len - (1 << 31), *serialized.get(i)?));
                // println!("got run");
                i += 1;
            } else {
                encoded.push(RunSegment::NonRun(serialized[i..(i + seg_len as usize)].to_vec()));
                // println!("got non run");
                i += seg_len as usize;
            }
        }
        Some(RleEncoding { len, encoded })
    }

    pub fn decode(&self, size: usize) -> Vec<u8> {
        assert!(size >= self.len);
        let mut decoded = vec![0; size];
        let mut idx = 0;
        for seg in &self.encoded {
            match seg {
                RunSegment::NonRun(vec) => {
                    for j in 0..vec.len() {
                        decoded[idx] = vec[j];
                        idx += 1;
                    }
                },
                RunSegment::Run(len, val) => {
                    if *val == 0 {
                        idx += *len as usize;
                    } else {
                        for _ in 0..(*len) {
                            decoded[idx] = *val;
                            idx += 1;
                        }
                    }
                }
            }
        }
        decoded
    }
}

// const DUMP_THREADS: usize = 50;

pub struct DiskDeque<T> {
    front: VecDeque<T>,
    back: VecDeque<T>,
    save_start: usize,
    save_end: usize,
    dir: PathBuf,
    capacity: usize,
}

impl<T: Serialize + DeserializeOwned> DiskDeque<T> {
    pub fn new<P: Into<PathBuf>>(dir: P, capacity: usize) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        DiskDeque {
            front: VecDeque::new(),
            back: VecDeque::new(),
            save_start: 0,
            save_end: 0,
            dir: dir,
            capacity,
        }
    }

    pub async fn pop(&mut self) -> Option<T> {
        if let Some(x) = self.front.pop_front() {
            return Some(x);
        }
        if self.save_start < self.save_end {
            self.front = self.load_swap(self.save_start).await;
            self.save_start += 1;
            return self.front.pop_front();
        }
        self.back.pop_front()
    }

    pub async fn push(&mut self, x: T) {
        self.back.push_back(x);
        if self.back.len() >= self.capacity {
            self.dump_swap(self.save_end).await;
            self.back.clear();
            self.save_end += 1;
        }
    }

    fn swap_path(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("swap{}", idx))
    }

    async fn load_swap(&self, idx: usize) -> VecDeque<T> {
        let mut file = tokio::fs::File::open(self.swap_path(idx)).await.unwrap();
        let mut contents = vec![];
        file.read_to_end(&mut contents).await.unwrap();
        deserialize(&contents).unwrap()
    }

    async fn dump_swap(&self, idx: usize) {
        let mut file = tokio::fs::File::create(self.swap_path(idx)).await.unwrap();
        file.write_all(&serialize(&self.back).unwrap()).await.unwrap();
        file.sync_all().await.unwrap();
    }
}

pub struct DiskMultiMap {
    cache: BTreeMap<String, RunEncoder>,
    dir: PathBuf,
    db_count: usize,
}

impl DiskMultiMap {
    pub fn new<P: Into<PathBuf>>(dir: P) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        DiskMultiMap {
            cache: BTreeMap::new(),
            dir: dir,
            db_count: 0,
        }
    }

    pub fn add(&mut self, key: String, id: usize, factor: u8) {
        let encoder = match self.cache.get_mut(&key) {
            Some(encoder) => encoder,
            None => {
                self.cache.insert(key.clone(), RunEncoder::new());
                self.cache.get_mut(&key).unwrap()
            },
        };
        encoder.add(id, factor);
    }

    pub async fn dump(&mut self) {
        let mut cache = BTreeMap::new();
        std::mem::swap(&mut self.cache, &mut cache);
        let path = self.db_path(self.db_count);
        self.db_count += 1;
        tokio::spawn(async move { Self::dump_cache(cache, path).await });
    }

    fn db_path(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("db{}", idx))
    }

    async fn dump_cache(cache: BTreeMap<String, RunEncoder>, path: PathBuf) {
        println!("writing to disk: {:?}", path);
        fs::create_dir_all(&path).unwrap();
        let mut data = Vec::new();
        let mut metadata: BTreeMap<String, (u32, u32)> = BTreeMap::new();
        let mut offset: u32 = 0;
        for (key, encoder) in cache {
            let bytes = serialize(&encoder.encode()).unwrap();
            metadata.insert(key, (offset, bytes.len() as u32));
            data.extend_from_slice(&bytes);
            offset += bytes.len() as u32;
        }
        sync_write(path.join("metadata"), &serialize(&metadata).unwrap()).await;
        sync_write(path.join("data"), &data).await;
        println!("finished writing to {:?}", path);
    }
}

async fn sync_write<P: AsRef<Path>>(path: P, bytes: &[u8]) {
    let mut file = tokio::fs::File::create(path).await.unwrap();
    file.write_all(bytes).await.unwrap();
    file.sync_all().await.unwrap();
}

pub struct DiskMap<V> {
    dir: PathBuf,
    cache: Vec<V>,
    db_count: usize,
    capacity: usize,
}

impl<V: Serialize + DeserializeOwned + Send + 'static> DiskMap<V> {
    pub fn new<P: Into<PathBuf>>(dir: P, capacity: usize) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        DiskMap {
            dir,
            capacity,
            cache: Vec::with_capacity(capacity),
            db_count: 0,
        }
    }

    pub fn push(&mut self, value: V) {
        self.cache.push(value);
    }

    pub fn dump(&mut self) {
        let mut cache = Vec::with_capacity(self.capacity);
        std::mem::swap(&mut self.cache, &mut cache);
        let path = self.db_path(self.db_count);
        self.db_count += 1;
        thread::spawn(move || Self::dump_cache(cache, path));
    }

    fn db_path(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("db{}", idx))
    }

    fn dump_cache(cache: Vec<V>, path: PathBuf) {
        // println!("writing to disk: {:?}", path);
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(&serialize(&cache).unwrap()).unwrap();
        file.sync_all().unwrap();
        // println!("finished writing to {:?}", path);
    }
}

pub struct TaskPool<T> {
    pool: Arc<Vec<Mutex<DiskDeque<T>>>>,
}

pub struct TaskHandler<T> {
    pool: Arc<Vec<Mutex<DiskDeque<T>>>>,
    i: usize,
}

impl<T: Serialize + DeserializeOwned> TaskPool<T> {
    pub fn new<P: AsRef<Path>>(dir: P, capacity: usize, n: usize) -> TaskPool<T> {
        let mut pool = Vec::new();
        let dir = dir.as_ref();
        for i in 0..n {
            pool.push(Mutex::new(
                DiskDeque::new(dir.join(format!("thread{}", i)), capacity)
            ));
        }
        TaskPool { pool: Arc::new(pool) }
    }

    pub fn handler(&self, i: usize) -> TaskHandler<T> {
        TaskHandler { pool: self.pool.clone(), i }
    }

    pub async fn push(&self, task: T) {
        let i = rand::thread_rng().gen::<usize>() % self.pool.len();
        self.pool[i].lock().await.push(task).await;
    }
}

impl<T: Serialize + DeserializeOwned> TaskHandler<T> {
    pub async fn pop(&self) -> Option<T> {
        let own_task = {
            self.pool[self.i].try_lock().ok()?.pop().await
        };
        match own_task {
            Some(task) => Some(task),
            None => self.steal().await,
        }
    }

    async fn steal(&self) -> Option<T> {
        let j = rand::thread_rng().gen::<usize>() % self.pool.len();
        self.try_steal(j).await
    }

    async fn try_steal(&self, j: usize) -> Option<T> {
        self.pool[j % self.pool.len()].try_lock().ok()?.pop().await
    }

    pub async fn push(&self, task: T, d: usize) {
        self.pool[d % self.pool.len()].lock().await.push(task).await;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Posting {
    pub url_id: u32,
    pub count: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UrlMeta {
    pub url: String,
    pub n_terms: u8,
}

pub struct IndexShard {
    index: fs::File,
    headers: BTreeMap<String, (u32, u32)>,
    meta_path: PathBuf,
    term_counts: Vec<u8>,
    core: String,
    idx: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexHeader {
    headers: BTreeMap<String, (u32, u32)>,
    term_counts: Vec<u8>,
    core: String,
    idx: usize,
}

impl IndexShard {
    pub fn open<P: Into<PathBuf>, Q: Into<PathBuf>>(index_dir: P, meta_dir: Q, core: String, idx: usize) -> Option<IndexShard> {
        let index_dir = index_dir.into().join(&core).join(format!("db{}", idx));
        let meta_path = meta_dir.into().join(&core).join(format!("db{}", idx));
        let headers = deserialize(&fs::read(index_dir.join("metadata")).ok()?).ok()?;
        let index = fs::File::open(index_dir.join("data")).ok()?;
        let meta: Vec<UrlMeta> = deserialize(&fs::read(&meta_path).ok()?).ok()?;
        let term_counts = meta.iter().map(|url_meta| url_meta.n_terms).collect::<Vec<_>>();
        Some(Self { index, headers, meta_path, term_counts, core, idx })
    }

    pub fn find_idxs<P: AsRef<Path>>(index_dir: P) -> Vec<(String, usize)> {
        let mut idxs = Vec::new();
        let index_dir = index_dir.as_ref();
        for core_entry in fs::read_dir(&index_dir).unwrap() {
            for db_entry in fs::read_dir(core_entry.unwrap().path()).unwrap() {
                let path = db_entry.unwrap().path();
                let filename = path.file_name().unwrap().to_str().unwrap();
                let idx = filename[2..].parse::<usize>().unwrap();
                let parent = String::from(path.parent().unwrap().file_name().unwrap().to_str().unwrap());
                idxs.push((parent, idx));
            }
        }
        idxs
    }

    pub fn into_header(self) -> IndexHeader {
        IndexHeader { headers: self.headers, term_counts: self.term_counts, core: self.core, idx: self.idx }
    }

    pub fn from_header<P: Into<PathBuf>, Q: Into<PathBuf>>(header: IndexHeader, index_dir: P, meta_dir: Q) -> IndexShard {
        let IndexHeader { headers, term_counts, core, idx } = header;
        let index_dir = index_dir.into().join(&core).join(format!("db{}", idx));
        let meta_path = meta_dir.into().join(&core).join(format!("db{}", idx));
        let index = fs::File::open(index_dir.join("data")).unwrap();
        Self { index, headers, meta_path, term_counts, core, idx }
    }

    pub fn num_terms(&self) -> usize {
        self.headers.len()
    }

    pub fn term_counts(&self) -> &[u8] {
        &self.term_counts
    }

    pub fn get_postings(&mut self, term: &str) -> Option<RleEncoding> {
        let (offset, len) = self.headers.get(term)?;
        self.index.seek(SeekFrom::Start(*offset as u64)).ok()?;
        let mut bytes = vec![0; *len as usize];
        self.index.read_exact(&mut bytes).unwrap();
        let postings: RleEncoding = deserialize(&bytes).ok()?;
        Some(postings)
    }

    pub fn open_meta(&self) -> Vec<UrlMeta> {
        deserialize(&fs::read(&self.meta_path).unwrap()).unwrap()
    }
}
