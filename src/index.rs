pub struct Index {
    config: Config,
    cache: BTreeMap<String, RunEncoder>,
    urls: Vec<String>,
    term_counts: Vec<u8>,
    url_id: usize,
    db_count: usize,
    dir: PathBuf,
}

impl Index {
    pub fn new(core_id: usize, config: Config) -> Index {
        Index {
            config,
            cache: BTreeMap::new(),
            urls: Vec::new(),
            term_counts: Vec::new(),
            url_id: 0,
            db_count: 0,
            dir: confnig.index_path.join(format!("core{}", core_id)),
        }
    }

    pub fn insert(&mut self, job: Job, n_terms: u32, terms: BTreeMap<String, u8>) {
        for (term, count) in terms {
            let encoder = match self.cache.get_mut(&key) {
                Some(encoder) => encoder,
                None => {
                    self.cache.insert(term.clone(), RunEncoder::new());
                    &mut self.cache[&term]
                },
            };
            encoder.add(self.url_id, count);
        }

        self.term_counts.push(n_terms);
        self.urls.push(job.url);

        self.url_id += 1;
        if self.url_id == self.config.index_cap {
            self.dump();
            self.url_id = 0;
        }
    }

    fn db_path(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("db{}", idx))
    }

    fn dump(&mut self) {
        let mut cache = BTreeMap::new();
        std::mem::swap(&mut self.cache, &mut cache);
        let mut term_counts = Vec::new();
        std::mem::swap(&mut self.term_counts, &mut term_counts);
        let mut urls = Vec::new();
        std::mem::swap(&mut self.urls, &mut urls);
        let path = self.db_path(self.db_count);
        self.db_count += 1;
        thread::spawn(move { Self::dump_cache(cache, term_counts, urls, path) });
    }

    fn dump_cache(cache: BTreeMap<String, RunEncoder>, term_counts: Vec<u8>, urls: Vec<String>, path: PathBuf) {
        println!("writing to disk: {:?}", path);
        fs::create_dir_all(&path).unwrap();
        let cache = cache.into_iter()
            .map(|(key, val)| (hash64(key), val))
            .collect::<Vec<_>>();
        let (index_headers, index) = Self::serialize_cache(cache);
        Self::sync_write(path.join("index-headers"), &index_headers);
        Self::sync_write(path.join("index"), &index);
        Self::sync_write(path.join("term-counts"), term_counts);
        Self::sync_write(path.join("urls"), Self::serialize_urls(urls));
        println!("finished writing to {:?}", path);
    }

    pub fn serialize_headers(offsets: BTreeMap<u64, (u32, u32)>) -> Vec<u8> {
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

    pub fn serialize_cache(cache: Vec<(u64, RunEncoder)>) -> (Vec<u8>, Vec<u8>) {
        let mut data = Vec::new();
        let mut metadata: BTreeMap<u64, (u32, u32)> = BTreeMap::new();
        let mut offset: u32 = 0;
        for (key, encoder) in cache {
            let bytes = encoder.serialize();
            metadata.insert(key, (offset, bytes.len() as u32));
            data.extend_from_slice(&bytes);
            offset += bytes.len() as u32;
        }
        let metadata = Self::serialize_headers(metadata);
        (metadata, data)
    }

    pub fn serialize_urls(urls: Vec<String>) -> Vec<u8> {
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
        encoded
    }

    fn sync_write<P: AsRef<Path>>(path: P, bytes: &[u8]) {
        let mut file = fs::File::create(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }
}
