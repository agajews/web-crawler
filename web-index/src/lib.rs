use sled;
use bincode::{serialize, deserialize};
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::fs;
use std::thread;
use rand;
use rand::Rng;
use std::collections::{VecDeque, BTreeMap};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::sync::Arc;
use tokio::sync::Mutex;

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
        let mut file = File::open(self.swap_path(idx)).await.unwrap();
        let mut contents = vec![];
        file.read_to_end(&mut contents).await.unwrap();
        deserialize(&contents).unwrap()
    }

    async fn dump_swap(&self, idx: usize) {
        let mut file = File::create(self.swap_path(idx)).await.unwrap();
        file.write_all(&serialize(&self.back).unwrap()).await.unwrap()
    }
}

pub struct DiskMultiMap<K, V> {
    cache: BTreeMap<K, Vec<V>>,
    dir: PathBuf,
}

impl<K: Serialize + DeserializeOwned + Ord + Send + 'static, V: Serialize + DeserializeOwned + Send + 'static> DiskMultiMap<K, V> {
    pub fn new<P: Into<PathBuf>>(dir: P) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        DiskMultiMap {
            cache: BTreeMap::new(),
            dir: dir,
        }
    }

    pub fn add(&mut self, key: K, value: V) {
        match self.cache.get_mut(&key) {
            Some(vec) => vec.push(value),
            None => { self.cache.insert(key, vec![value]); () },
        }
    }

    pub fn dump(&mut self, idx: usize) {
        let mut cache = BTreeMap::new();
        std::mem::swap(&mut self.cache, &mut cache);
        let path = self.db_path(idx);
        thread::spawn(move || Self::dump_cache(cache, path));
    }

    fn db_path(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("db{}", idx))
    }

    fn dump_cache(cache: BTreeMap<K, Vec<V>>, path: PathBuf) {
        println!("writing to disk: {:?}", path);
        let db = sled::open(&path).unwrap();
        let mut batch = sled::Batch::default();
        for (key, vec) in cache {
            batch.insert(serialize(&key).unwrap(), serialize(&vec).unwrap());
        }
        db.apply_batch(batch).unwrap();
        println!("finished writing to {:?}", path);
    }
}

pub struct DiskMap<K, V> {
    dir: PathBuf,
    cache: BTreeMap<K, V>,
}

impl<K: Serialize + DeserializeOwned + Ord + Send + 'static, V: Serialize + DeserializeOwned + Send + 'static> DiskMap<K, V> {
    pub fn new<P: Into<PathBuf>>(dir: P) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        DiskMap {
            dir,
            cache: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    pub fn dump(&mut self, idx: usize) {
        let mut cache = BTreeMap::new();
        std::mem::swap(&mut self.cache, &mut cache);
        let path = self.db_path(idx);
        thread::spawn(move || Self::dump_cache(cache, path));
    }

    fn db_path(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("db{}", idx))
    }

    fn dump_cache(cache: BTreeMap<K, V>, path: PathBuf) {
        println!("writing to disk: {:?}", path);
        let db = sled::open(&path).unwrap();
        let mut batch = sled::Batch::default();
        for (key, val) in cache {
            batch.insert(serialize(&key).unwrap(), serialize(&val).unwrap());
        }
        db.apply_batch(batch).unwrap();
        println!("finished writing to {:?}", path);
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
        // for j in (self.i + 1)..=(self.i + self.pool.len()) {
        //     if let Some(task) = self.try_steal(j).await {
        //         return Some(task);
        //     }
        // }
        // None
    }

    async fn try_steal(&self, j: usize) -> Option<T> {
        self.pool[j % self.pool.len()].try_lock().ok()?.pop().await
    }

    pub async fn push(&self, task: T, d: usize) {
        // let j = rand::thread_rng().gen::<usize>() % self.pool.len();
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
}

pub struct IndexShard {
    index: sled::Db,
    meta: sled::Db,
}

impl IndexShard {
    pub fn open<P: AsRef<Path>, Q: AsRef<Path>>(index_dir: P, meta_dir: Q, idx: usize) -> IndexShard {
        let index = sled::Config::default()
            .path(index_dir.as_ref().join(format!("db{}", idx)))
            .read_only(true)
            .open()
            .unwrap();
        let meta = sled::Config::default()
            .path(meta_dir.as_ref().join(format!("db{}", idx)))
            .read_only(true)
            .open()
            .unwrap();
        Self { index, meta }
    }

    pub fn open_all<P: AsRef<Path>, Q: AsRef<Path>>(index_dir: P, meta_dir: Q) -> Vec<IndexShard> {
        let mut idxs = Vec::new();
        for core_entry in fs::read_dir(&index_dir).unwrap() {
            for db_entry in fs::read_dir(core_entry.unwrap().path()).unwrap() {
                let path = db_entry.unwrap().path();
                let filename = path.file_name().unwrap().to_str().unwrap();
                let idx = filename[2..].parse::<usize>().unwrap();
                idxs.push(idx);
            }
        }
        idxs.sort();
        let mut shards = Vec::new();
        for idx in &idxs[0..(idxs.len() - 1)] {
            shards.push(Self::open(&index_dir, &meta_dir, *idx));
        }
        shards
    }

    pub fn get_postings(&self, term: &str) -> Vec<Posting> {
        match self.index.get(&serialize(term).unwrap()).unwrap() {
            Some(bytes) => deserialize(&bytes).unwrap(),
            None => Vec::new(),
        }
    }

    pub fn get_meta(&self, url_id: u32) -> UrlMeta {
        deserialize(&self.meta.get(&serialize(&url_id).unwrap()).unwrap().unwrap()).unwrap()
    }
}
