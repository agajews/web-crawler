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
    capacity: usize,  // in bytes, not in number of elements
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

    pub fn add(&mut self, key: String, id: usize) {
        let vec = match self.cache.get_mut(&key) {
            Some(vec) => vec,
            None => {
                self.cache.insert(key.clone(), vec![0; self.capacity]);
                self.cache.get_mut(&key).unwrap()
            },
        };
        let byte_idx = id >> 3;
        vec[byte_idx] |= 1 << (id % 8);
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
        let mut metadata = BTreeMap::new();
        let mut offset = 0;
        for (key, vec) in cache {
            let bytes = serialize(&vec).unwrap();
            metadata.insert(key, (offset, bytes.len()));
            data.extend_from_slice(&bytes);
            offset += bytes.len();
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

pub struct DiskMap<K, V> {
    dir: PathBuf,
    cache: BTreeMap<K, V>,
    db_count: usize,
}

impl<K: Serialize + DeserializeOwned + Ord + Send + 'static, V: Serialize + DeserializeOwned + Send + 'static> DiskMap<K, V> {
    pub fn new<P: Into<PathBuf>>(dir: P) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        DiskMap {
            dir,
            cache: BTreeMap::new(),
            db_count: 0,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    pub fn dump(&mut self) {
        let mut cache = BTreeMap::new();
        std::mem::swap(&mut self.cache, &mut cache);
        let path = self.db_path(self.db_count);
        self.db_count += 1;
        thread::spawn(move || Self::dump_cache(cache, path));
    }

    fn db_path(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("db{}", idx))
    }

    fn dump_cache(cache: BTreeMap<K, V>, path: PathBuf) {
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
    pub n_terms: u32,
    pub term_counts: BTreeMap<String, u32>,
}

pub struct IndexShard {
    index: File,
    headers: BTreeMap<String, (usize, usize)>,
    meta_path: PathBuf,
    pub term_counts: BTreeMap<u32, u32>,
}

impl IndexShard {
    pub fn open<P: Into<PathBuf>, Q: Into<PathBuf>>(index_dir: P, meta_dir: Q, core: &str, idx: usize) -> Option<IndexShard> {
        let index_dir = index_dir.into().join(core).join(format!("db{}", idx));
        let meta_path = meta_dir.into().join(core).join(format!("db{}", idx));
        let headers = deserialize(&fs::read(index_dir.join("metadata")).ok()?).ok()?;
        let index = File::open(index_dir.join("data")).ok()?;
        let meta: BTreeMap<u32, UrlMeta> = deserialize(&fs::read(&meta_path).ok()?).ok()?;
        let term_counts = meta.into_iter()
            .map(|(id, url_meta)| (id, url_meta.n_terms))
            .collect::<BTreeMap<_, _>>();
        Some(Self { index, headers, meta_path, term_counts })
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

    pub fn get_postings(&mut self, term: &str) -> Vec<Posting> {
        match self.read_postings(term) {
            Some(postings) => postings,
            None => Vec::new(),
        }
    }

    fn read_postings(&mut self, term: &str) -> Option<Vec<Posting>> {
        let (offset, len) = self.headers.get(term)?;
        self.index.seek(SeekFrom::Start(*offset as u64)).ok()?;
        let mut bytes = vec![0; *len];
        self.index.read_exact(&mut bytes).unwrap();
        let postings: Vec<Posting> = deserialize(&bytes).ok()?;
        Some(postings)
    }

    pub fn open_meta(&self) -> BTreeMap<u32, UrlMeta> {
        let meta = deserialize(&fs::read(&self.meta_path).unwrap()).unwrap();
        meta
    }
}
