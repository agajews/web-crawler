pub struct Page {
    capacity: usize,
    entries: Vec<(String, u32, bool)>,
    pub bounds: PageBounds,
}

impl Page {
    pub fn increment(&mut self, job: Job) -> Option<Page> {
        assert!(job.url >= self.bounds.left && job.url < self.bounds.right);
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
}
