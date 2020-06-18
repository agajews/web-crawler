use sled;
use bincode::{serialize, deserialize};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
enum Block {
    Meta(u32, u32),
    Data(u32, [(u32, u32); 32]),
}

pub struct Meta {
    db: sled::Db,
}

impl Meta {
    pub fn new<P: AsRef<Path>>(filename: P) -> Self {
        Self { db: sled::open(filename).unwrap() }
    }

    pub fn get(&self, id: u32) -> Option<(u32, String)> {
        let key = serialize(&id).unwrap();
        let data = self.db.get(key).unwrap()?;
        Some(deserialize(&data).unwrap())
    }

    pub fn insert(&self, id: u32, data: (u32, String)) {
        let key = serialize(&id).unwrap();
        let data = serialize(&data).unwrap();
        self.db.insert(key, data).unwrap();
    }
}

pub struct Index {
    db: sled::Db,
}

impl Index {
    pub fn new<P: AsRef<Path>>(filename: P) -> Self {
        Self { db: sled::open(filename).unwrap() }
    }

    fn get(&self, term: &str, block: Option<u32>) -> Option<Block> {
        let key = serialize(&(String::from(term), block)).unwrap();
        let data = self.db.get(key).unwrap()?;
        Some(deserialize::<Block>(&data).unwrap())
    }

    fn put(&self, term: &str, block: Option<u32>, contents: Block) {
        let key = serialize(&(String::from(term), block)).unwrap();
        let data = serialize::<Block>(&contents).unwrap();
        self.db.insert(key, data).unwrap();
    }

    fn create_term(&self, term: &str) {
        self.put(term, None, Block::Meta(1, 1));
    }

    fn update_term(&self, term: &str, n_docs: u32, n_blocks: u32) {
        self.put(term, None, Block::Meta(n_docs, n_blocks));
     }

    fn create_block(&self, block: u32, term: &str, data: (u32, u32)) {
        let mut data_arr = [(0, 0); 32];
        data_arr[0] = data;
        self.put(term, Some(block), Block::Data(1, data_arr));
    }

    fn block_insert(&self, block: u32, term: &str, data: (u32, u32)) -> bool {
        let (count, mut data_arr) = match self.get(term, Some(block)) {
            Some(Block::Data(c, a)) => (c, a),
            _ => return false,
        };
        if count == data_arr.len() as u32 {
            return false;
        }
        data_arr[count as usize] = data;
        self.put(term, Some(block), Block::Data(count + 1, data_arr));
        true
    }

    pub fn insert(&self, term: &str, data: (u32, u32)) {
        match self.get(term, None) {
            Some(Block::Meta(n_docs, n_blocks)) => {
                self.update_term(term, n_docs + 1, n_blocks);
                if !self.block_insert(n_blocks - 1, term, data) {
                    self.create_block(n_blocks, term, data);
                    self.update_term(term, n_docs + 1, n_blocks + 1);
                }
            },
            _ => {
                self.create_block(0, term, data);
                self.create_term(term);
            },
        };
    }

    pub fn query(&self, term: &str) -> Option<QueryIterator> {
        let n_docs = match self.get(term, None) {
            Some(Block::Meta(n_docs, _n_blocks)) => n_docs,
            _ => return None,
        };
        Some(QueryIterator {
            index: &self,
            term: String::from(term),
            length: n_docs,
            block: 0,
            idx: 0,
        })
    }
}

pub struct QueryIterator<'a> {
    index: &'a Index,
    term: String,
    length: u32,
    block: u32,
    idx: u32,
}

impl Iterator for QueryIterator<'_> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        let (count, data_arr) = match self.index.get(&self.term, Some(self.block)) {
            Some(Block::Data(count, data_arr)) => (count, data_arr),
            _ => return None,
        };
        self.idx += 1;
        if self.idx == count {
            self.idx = 0;
            self.block += 1;
        }
        self.length -= 1;
        Some(data_arr[self.idx as usize])
    }
}

impl ExactSizeIterator for QueryIterator<'_> {
    fn len(&self) -> usize {
        self.length as usize
    }
}
