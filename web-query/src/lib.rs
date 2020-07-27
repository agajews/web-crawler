use std::fs;
use std::io::SeekFrom;


pub struct IndexShard {
    index: fs::File,
    index_headers: Vec<(u64, (u32, u32))>,
    urls: fs::File,
    url_headers: Vec<(u32, u32)>,
    n_urls: usize,
    term_counts: Vec<u8>,
}

impl IndexShard {
    pub fn open(path: &Path) -> Result<IndexShard, Box<dyn Error>> {
        let index_headers = Self::deserialize_index_headers(&fs::read(path.join("index-headers"))?)?;
        let index = fs::File::open(&path.join("index"))?;
        let mut urls = fs::File::open(&path.join("urls"))?;
        let (n_urls, url_headers) = Self::deserialize_url_headers(&mut urls)?;
        let term_counts = fs::read(&path.join("term-counts"))?;
        Some(Self { index, index_headers, urls, n_urls, url_headers, term_counts })
    }

    fn deserialize_index_headers(bytes: &[u8]) -> Result<Vec<(u64, (u32, u32))>, Box<dyn Error>> {
        let len = u32::from_be_bytes(bytes[0..4].try_into()?);
        let mut headers = Vec::with_capacity(len as usize);
        let mut i = 4;
        for _ in 0..len {
            let key = u64::from_be_bytes(bytes[i..(i + 8)].try_into()?);
            i += 8;
            let offset = u32::from_be_bytes(bytes[i..(i + 4)].try_into()?);
            i += 4;
            let len = u32::from_be_bytes(bytes[i..(i + 4)].try_into()?);
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

    pub fn find_shards(index_dir: &Path) -> Vec<PathBuf> {
        let mut shards = Vec::new();
        for core_entry in fs::read_dir(&index_dir).unwrap() {
            for db_entry in fs::read_dir(core_entry.unwrap().path()).unwrap() {
                let path = db_entry.unwrap().path();
                shards.push(path);
            }
        }
        shards
    }

    pub fn num_terms(&self) -> usize {
        self.index_headers.len()
    }

    pub fn term_counts(&self) -> &[u8] {
        &self.term_counts
    }

    pub fn keys(&self) -> Vec<u64> {
        let keys = self.index_headers.iter()
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        keys
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
