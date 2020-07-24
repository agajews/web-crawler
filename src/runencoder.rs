pub struct RunEncoder {
    min_run_len: usize,
    len: usize,
    encoded: Vec<u8>,
    n_segs: usize,
    non_run: Vec<u8>,
    run: Vec<u8>,
}

impl RunEncoder {
    pub fn new(min_run_len: usize) -> RunEncoder {
        RunEncoder {
            min_run_len,
            encoded: Vec::new(),
            non_run: Vec::new(),
            run: Vec::new(),
            len: 0,
            n_segs: 0,
        }
    }

    pub fn add(&mut self, idx: usize, byte: u8) {
        assert!(idx >= self.len);
        if idx - self.len < self.min_run_len {
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
                if self.run.len() < self.min_run_len {
                    self.non_run.append(&mut self.run);
                } else {
                    self.sync_segs();
                }
                self.run.push(byte);
            }
        }
    }

    fn flush(&mut self) {
        if self.run.len() < self.min_run_len {
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
                if i + seg_len > serialized.len() {
                    return None;
                }
                decoded[k..(k + seg_len)].copy_from_slice(&serialized[i..(i + seg_len)]);
                i += seg_len;
                k += seg_len;
            }
        }
        Some(decoded)
    }
}

