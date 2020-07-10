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
use std::fs::File;
use std::io::SeekFrom;
use std::convert::TryInto;

fn rle_encode(vec: Vec<u8>) -> Vec<u8> {
    let mut i = 0;
    let mut encoded = Vec::new();
    let mut non_run = Vec::new();
    encoded.extend_from_slice(&(vec.len() as u32).to_be_bytes());
    while i < vec.len() {
        let start = i;
        let val = vec[i];
        while i < vec.len() && vec[i] == val && i - start < 127 {
            i += 1;
        }
        let is_run = i - start > 3;
        if non_run.len() == 127 || (is_run && non_run.len() > 0) {
            encoded.push((128 + non_run.len()) as u8);
            encoded.extend_from_slice(&non_run);
            non_run.clear();
        }
        if is_run {
            encoded.push((i - start) as u8);
            encoded.push(val);
        } else {
            i = start + 1;
            non_run.push(val);
        }
    }
    if non_run.len() > 0 {
        encoded.push((128 + non_run.len()) as u8);
        encoded.extend_from_slice(&non_run);
    }
    encoded
}

fn rle_decode(vec: Vec<u8>) -> Option<Vec<u8>> {
    let len = u32::from_be_bytes((&vec[0..4]).clone().try_into().unwrap());
    let mut decoded: Vec<u8> = vec![0; len as usize];
    let mut i = 4;
    let mut j = 0;
    while i < vec.len() {
        if vec[i] < 128 {
            let run_len = vec[i] as usize;
            i += 1;
            let run_val = *vec.get(i)?;
            if run_val != 0 {
                for _ in 0..run_len {
                    if j >= decoded.len() {
                        return None;
                    }
                    decoded[j] = run_val;
                    j += 1;
                }
            } else {
                j += run_len;
            }
            i += 1;
        } else {
            let non_run_len = (vec[i] - 128) as usize;
            i += 1;
            for _ in 0..non_run_len {
                if j >= decoded.len() {
                    return None;
                }
                decoded[j] = *vec.get(i)?;
                j += 1;
                i += 1;
            }
        }
    }
    Some(decoded)
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
    cache: BTreeMap<String, Vec<u8>>,
    dir: PathBuf,
    db_count: usize,
    capacity: usize,
}

impl DiskMultiMap {
    pub fn new<P: Into<PathBuf>>(dir: P, capacity: usize) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        DiskMultiMap {
            cache: BTreeMap::new(),
            dir: dir,
            db_count: 0,
            capacity: capacity,
        }
    }

    pub fn add(&mut self, key: String, id: usize, factor: u8) {
        let vec = match self.cache.get_mut(&key) {
            Some(vec) => vec,
            None => {
                self.cache.insert(key.clone(), vec![0; self.capacity]);
                self.cache.get_mut(&key).unwrap()
            },
        };
        vec[id] = factor;
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

    async fn dump_cache(cache: BTreeMap<String, Vec<u8>>, path: PathBuf) {
        println!("writing to disk: {:?}", path);
        fs::create_dir_all(&path).unwrap();
        let mut data = Vec::new();
        let mut metadata: BTreeMap<String, (u32, u32)> = BTreeMap::new();
        let mut offset: u32 = 0;
        for (key, vec) in cache {
            let bytes = rle_encode(vec);
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
}

pub struct IndexShard {
    index: File,
    headers: BTreeMap<String, (u32, u32)>,
    meta_path: PathBuf,
}

impl IndexShard {
    pub fn open<P: Into<PathBuf>, Q: Into<PathBuf>>(index_dir: P, meta_dir: Q, core: &str, idx: usize) -> Option<IndexShard> {
        let index_dir = index_dir.into().join(core).join(format!("db{}", idx));
        let meta_path = meta_dir.into().join(core).join(format!("db{}", idx));
        let headers = deserialize(&fs::read(index_dir.join("metadata")).ok()?).ok()?;
        let index = File::open(index_dir.join("data")).ok()?;
        let _meta: Vec<UrlMeta> = deserialize(&fs::read(&meta_path).ok()?).ok()?;
        Some(Self { index, headers, meta_path })
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

    pub fn num_terms(&self) -> usize {
        self.headers.len()
    }

    pub fn get_postings(&mut self, term: &str) -> Option<Vec<u8>> {
        let (offset, len) = self.headers.get(term)?;
        self.index.seek(SeekFrom::Start(*offset as u64)).ok()?;
        let mut bytes = vec![0; *len as usize];
        self.index.read_exact(&mut bytes).unwrap();
        let postings: Vec<u8> = rle_decode(bytes)?;
        Some(postings)
    }

    pub fn open_meta(&self) -> Vec<UrlMeta> {
        let meta = deserialize(&fs::read(&self.meta_path).unwrap()).unwrap();
        meta
    }
}
