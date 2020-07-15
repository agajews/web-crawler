use bincode::{serialize, deserialize};
use serde::Serialize;
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
use fasthash::metro::hash64;

pub struct RunEncoder {
    len: usize,
    encoded: Vec<u8>,
    n_segs: usize,
    non_run: Vec<u8>,
    run: Vec<u8>,
}

impl RunEncoder {
    pub fn new() -> RunEncoder {
        RunEncoder {
            encoded: Vec::new(),
            non_run: Vec::new(),
            run: Vec::new(),
            len: 0,
            n_segs: 0,
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
            self.push_run((idx - self.len) as u32, 0);
            self.run.push(byte);
        }
        self.len = idx + 1;
    }

    fn push_run(&mut self, len: u32, val: u8) {
        self.n_segs += 1;
        self.encoded.extend_from_slice(&(len + (1 << 31)).to_be_bytes());
        self.encoded.push(val);
    }

    fn push_non_run(&mut self) {
        self.n_segs += 1;
        self.encoded.extend_from_slice(&(self.non_run.len() as u32).to_be_bytes());
        self.encoded.extend_from_slice(&self.non_run);
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
            self.push_non_run();
            self.non_run.clear();
        }
        if self.run.len() > 0 {
            self.push_run(self.run.len() as u32, self.run[0]);
            self.run.clear();
        }
    }

    pub fn serialize(mut self) -> Vec<u8> {
        self.flush();
        let mut serialized = Vec::with_capacity(self.encoded.len() + 8);
        serialized.extend_from_slice(&(self.len as u32).to_be_bytes());
        serialized.extend_from_slice(&(self.n_segs as u32).to_be_bytes());
        serialized.extend_from_slice(&self.encoded);
        serialized
    }

    pub fn deserialize(serialized: Vec<u8>, size: usize) -> Option<Vec<u8>> {
        let len = u32::from_be_bytes(serialized[0..4].try_into().ok()?) as usize;
        assert!(size >= len);
        let n_segs = u32::from_be_bytes(serialized[4..8].try_into().ok()?);
        let mut decoded = vec![0; size];
        let mut i = 8;
        let mut k = 0;
        for _ in 0..n_segs {
            let seg_len = u32::from_be_bytes(serialized[i..(i + 4)].try_into().ok()?) as usize;
            i += 4;
            if seg_len >= (1 << 31) {
                let run_len = seg_len - (1 << 31);
                let val = *serialized.get(i)?;
                if val == 0 {
                    k += run_len as usize;
                } else {
                    for _ in 0..run_len {
                        decoded[k] = val;
                        k += 1;
                    }
                }
                i += 1;
            } else {
                if i + seg_len >= serialized.len() {
                    return None;
                }
                decoded[k..(k + seg_len)].copy_from_slice(&serialized[i..(i + seg_len)]);
                // for j in 0..(seg_len as usize) {
                //     decoded[k] = *serialized.get(i + j)?;
                //     k += 1;
                // }
                i += seg_len as usize;
                k += seg_len as usize;
            }
        }
        Some(decoded)
    }
}

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

    fn serialize_headers(offsets: BTreeMap<u64, (u32, u32)>) -> Vec<u8> {
        let offsets = offsets.iter().collect::<Vec<_>>();
        let mut encoded = Vec::with_capacity(4 + 16 * offsets.len());
        encoded.extend_from_slice(&(offsets.len() as u32).to_be_bytes());
        for (key, (offset, len)) in offsets {
            encoded.extend_from_slice(&key.to_be_bytes());
            encoded.extend_from_slice(&offset.to_be_bytes());
            encoded.extend_from_slice(&len.to_be_bytes());
        }
        encoded
    }

    async fn dump_cache(cache: BTreeMap<String, RunEncoder>, path: PathBuf) {
        println!("writing to disk: {:?}", path);
        fs::create_dir_all(&path).unwrap();
        let mut data = Vec::new();
        let mut metadata: BTreeMap<u64, (u32, u32)> = BTreeMap::new();
        let mut offset: u32 = 0;
        for (key, encoder) in cache {
            let bytes = encoder.serialize();
            metadata.insert(hash64(key), (offset, bytes.len() as u32));
            data.extend_from_slice(&bytes);
            offset += bytes.len() as u32;
        }
        sync_write(path.join("metadata"), &Self::serialize_headers(metadata)).await;
        sync_write(path.join("data"), &data).await;
        println!("finished writing to {:?}", path);
    }
}

async fn sync_write<P: AsRef<Path>>(path: P, bytes: &[u8]) {
    let mut file = tokio::fs::File::create(path).await.unwrap();
    file.write_all(bytes).await.unwrap();
    file.sync_all().await.unwrap();
}

pub struct DiskMeta {
    dir: PathBuf,
    urls: Vec<String>,
    term_counts: Vec<u8>,
    db_count: usize,
    capacity: usize,
}

impl DiskMeta {
    pub fn new<P: Into<PathBuf>>(dir: P, capacity: usize) -> Self {
        let dir = dir.into();
        fs::create_dir_all(&dir).unwrap();
        Self {
            dir,
            capacity,
            urls: Vec::with_capacity(capacity),
            term_counts: Vec::with_capacity(capacity),
            db_count: 0,
        }
    }

    pub fn push(&mut self, url: String, n_terms: u8) {
        self.urls.push(url);
        self.term_counts.push(n_terms);
    }

    pub fn dump(&mut self) {
        let mut urls = Vec::with_capacity(self.capacity);
        let mut term_counts = Vec::with_capacity(self.capacity);
        std::mem::swap(&mut self.urls, &mut urls);
        std::mem::swap(&mut self.term_counts, &mut term_counts);
        let db_dir = self.dir.join(format!("db{}", self.db_count));
        let url_path = db_dir.join("urls");
        let term_path = db_dir.join("terms");
        self.db_count += 1;
        thread::spawn(move || {
            fs::create_dir_all(&db_dir).unwrap();
            Self::dump_urls(url_path, urls);
            Self::write_and_sync(term_path, term_counts);
        });
    }

    fn dump_urls(url_path: PathBuf, urls: Vec<String>) {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(urls.len() as u32).to_be_bytes());
        let mut i: u32 = 0;
        for url in &urls {
            encoded.extend_from_slice(&i.to_be_bytes());
            encoded.extend_from_slice(&(url.len() as u32).to_be_bytes());
            i += url.len() as u32;
        }
        for url in urls {
            encoded.extend_from_slice(url.as_bytes());
        }
        Self::write_and_sync(url_path, encoded);
    }

    fn write_and_sync(path: PathBuf, bytes: Vec<u8>) {
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
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

pub struct IndexShard {
    index: fs::File,
    index_headers: Vec<(u64, (u32, u32))>,
    urls: fs::File,
    url_headers: Vec<(u32, u32)>,
    n_urls: usize,
    term_counts: Vec<u8>,
}

impl IndexShard {
    pub fn open<P: Into<PathBuf>, Q: Into<PathBuf>>(index_dir: P, meta_dir: Q, core: String, idx: usize) -> Option<IndexShard> {
        let index_dir = index_dir.into().join(&core).join(format!("db{}", idx));
        let meta_path = meta_dir.into().join(&core).join(format!("db{}", idx));
        let index_headers = match fs::read(index_dir.join("metadata")).ok() {
            Some(bytes) => match Self::deserialize_index_headers(&bytes) {
                Some(headers) => headers,
                None => { println!("failed to deserialize headers for {}:{}", core, idx); return None; },
            },
            None => { println!("failed to read headers for {}:{}", core, idx); return None; },
        };
        let index = match fs::File::open(index_dir.join("data")).ok() {
            Some(file) => file,
            None => { println!("failed to read index for {}:{}", core, idx); return None; },
        };
        let mut urls = match fs::File::open(&meta_path.join("urls")) {
            Ok(file) => file,
            Err(err) => { println!("failed to read urls for {}:{}, {}", core, idx, err); return None; },
        };
        let (n_urls, url_headers) = match Self::deserialize_url_headers(&mut urls) {
            Some(headers) => headers,
            None => { println!("failed to deserialize url headers for {}:{}", core, idx); return None; },
        };
        let term_counts = match fs::read(&meta_path.join("terms")).ok() {
            Some(terms) => terms,
            None => { println!("failed to read term counts for {}:{}", core, idx); return None; },
        };
        Some(Self { index, index_headers, urls, n_urls, url_headers, term_counts })
    }

    fn deserialize_index_headers(bytes: &[u8]) -> Option<Vec<(u64, (u32, u32))>> {
        let len = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let mut headers = Vec::with_capacity(len as usize);
        let mut i = 4;
        for _ in 0..len {
            let key = u64::from_be_bytes(bytes[i..(i + 8)].try_into().ok()?);
            i += 8;
            let offset = u32::from_be_bytes(bytes[i..(i + 4)].try_into().ok()?);
            i += 4;
            let len = u32::from_be_bytes(bytes[i..(i + 4)].try_into().ok()?);
            i += 4;
            headers.push((key, (offset, len)));
        }
        Some(headers)
    }

    fn deserialize_url_headers(urls: &mut fs::File) -> Option<(usize, Vec<(u32, u32)>)> {
        let mut bytes = [0; 4];
        urls.read_exact(&mut bytes).ok()?;
        let n_urls = u32::from_be_bytes(bytes) as usize;
        let mut bytes = vec![0; n_urls * 8];
        urls.read_exact(&mut bytes).ok()?;
        let mut headers = Vec::with_capacity(n_urls);
        let mut i = 0;
        for _ in 0..n_urls {
            let offset = u32::from_be_bytes(bytes[i..(i + 4)].try_into().ok()?);
            i += 4;
            let len = u32::from_be_bytes(bytes[i..(i + 4)].try_into().ok()?);
            i += 4;
            headers.push((offset, len));
        }
        Some((n_urls, headers))
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
        self.index_headers.len()
    }

    pub fn term_counts(&self) -> &[u8] {
        &self.term_counts
    }

    fn search_for_term(&self, key: u64) -> Option<(u32, u32)> {
        let idx = self.index_headers.binary_search_by_key(&key, |(k, (_, _))| *k).ok()?;
        let (_, (offset, len)) = self.index_headers[idx];
        Some((offset, len))
    }

    pub fn get_postings(&mut self, term: &str, size: usize) -> Option<Vec<u8>> {
        let (offset, len) = self.search_for_term(hash64(term))?;
        self.index.seek(SeekFrom::Start(offset as u64)).ok()?;
        let mut bytes = vec![0; len as usize];
        self.index.read_exact(&mut bytes).ok()?;
        RunEncoder::deserialize(bytes, size)
    }

    pub fn get_url(&mut self, idx: usize) -> Option<String> {
        let (offset, len) = self.url_headers.get(idx)?;
        let header_len = 4 + self.n_urls * 8;
        let offset = header_len + *offset as usize;
        self.urls.seek(SeekFrom::Start(offset as u64)).ok()?;
        let mut bytes = vec![0; *len as usize];
        self.urls.read_exact(&mut bytes).ok()?;
        Some(String::from_utf8(bytes).ok()?)
    }
}
