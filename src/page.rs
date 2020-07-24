pub struct Page {
    capacity: usize,
    entries: Vec<(String, u32, bool)>,
    pub bounds: PageBounds,
}

impl Page {
    pub fn init(config: &Config) {
        Page {
            capacity: config.page_capacity,
            entries: Vec::new(),
            bounds: PageBounds::new(Marker::NegInf, Marker::PosInf),
        }
    }

    pub fn increment(&mut self, job: Job) -> Option<Page> {
        assert!(job.url >= Marker::finite(self.bounds.left) && job.url < Marker::finite(self.bounds.right));
        match self.entries.binary_search_by_key(job.url, |(url, _, _)| url) {
            Ok(i) => self.entries[i].1 += 1,
            Err(i) => self.entries.insert(i, (job.url, 1, false)),
        }
        if self.entries.len() > self.capacity {
            return Some(self.split());
        }
        return None;
    }

    fn split(&mut self) -> Page {
        let new_entries = self.entries.split_off(self.entries.len() / 2);
        let new_bounds = PageBounds::new(Marker::Finite(new_entries[0].0), self.bounds.right);
        self.bounds.right = Marker::Finite(new_entries[0].0);
        Page {
            capacity: self.capacity,
            entries: new_entries,
            bounds: new_bounds,
        }
    }

    fn pop(&mut self) -> Option<Job> {
        entries.iter_mut()
            .filter(|(_, _, popped)| !popped)
            .max_by(|(_, count, _)| count)
            .map(|(url, _, popped)| {
                *popped = true;
                Job::new(url)
            })
    }

    fn serialize(self, config: &Config) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + (1 + config.max_url_len) * 2 + (1 + 4 + 1 + config.max_url_len) * self.entries.len());
        bytes.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        let bounds_bytes = self.bounds.serialize();
        bytes.extend_from_slice(&(bounds_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&bounds_bytes);
        for (url, count, popped) in self.entries {
            bytes.extend_from_slice(&(url.len() as u8).to_be_bytes());
            bytes.extend_from_slice(&count.to_be_bytes());
            bytes.extend_from_slice(&(popped as u8).to_be_bytes());
            bytes.extend_from_slice(url.as_bytes());
        }
    }

    fn deserialize(config: &Config, bytes: Vec<u8>) -> Page {
        let n_entries = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let bounds_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let bounds = PageBounds::deserialize(bytes[12..(12 + bounds_len)]);
        let mut i = 12 + bounds_len;
        let mut entries = Vec::with_capacity(n_entries);
        for _ in 0..n_entries {
            let url_len = bytes[i] as usize;
            i += 1;
            let count = u32::from_be_bytes(bytes[i..(i + 4)].try_into().unwrap());
            i += 4;
            let popped = bytes[i] > 0;
            i += 1;
            let url = String::from_utf8(bytes[i..(i + url_len)]).unwrap();
            entries.push((url, count, popped));
        }
        Page {
            entries,
            bounds,
            capacity: config.page_capacity,
    }
}
