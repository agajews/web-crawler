use crate::pagebounds::{PageBounds,  PageBoundsCmp, Marker};
use crate::job::Job;
use crate::config::Config;
use crate::monitor::MonitorHandle;

use std::convert::TryInto;

#[derive(Clone)]
pub struct Page {
    capacity: usize,
    entries: Vec<(String, u32, bool)>,
    bounds: PageBounds,
    pub value: u32,
}

impl Page {
    pub fn init(config: &Config) -> Page {
        Page {
            capacity: config.page_capacity,
            entries: Vec::new(),
            bounds: PageBounds::new(Marker::NegInf, Marker::PosInf),
            value: 0,
        }
    }

    pub fn bounds_cmp(&self) -> PageBoundsCmp {
        PageBoundsCmp::Bounds(self.bounds.clone())
    }

    pub fn increment(&mut self, job: Job, monitor: &MonitorHandle) -> Option<Page> {
        // println!("incrementing {} on {:?}", job.url, self.bounds);
        // assert!(Marker::Finite(job.url.clone()) >= self.bounds.left && Marker::Finite(job.url.clone()) < self.bounds.right);
        let (new_value, popped) = match self.entries.binary_search_by_key(&job.url.as_str(), |(url, _, _)| &url.as_str()) {
            Ok(i) => {
                self.entries[i].1 += 1;
                (self.entries[i].1, self.entries[i].2)
            },
            Err(i) => {
                monitor.inc_seen_urls();
                self.entries.insert(i, (job.url, 1, false));
                (1, false)
            },
        };
        if !popped && new_value > self.value {
            self.value = new_value;
        }
        if self.entries.len() > self.capacity {
            return Some(self.split());
        }
        return None;
    }

    fn split(&mut self) -> Page {
        let new_entries = self.entries.split_off(self.entries.len() / 2);
        let new_bounds = PageBounds::new(Marker::Finite(new_entries[0].0.clone()), self.bounds.right.clone());
        self.bounds.right = Marker::Finite(new_entries[0].0.clone());
        self.value = Self::compute_value(&self.entries);
        let new_value = Self::compute_value(&new_entries);
        Page {
            capacity: self.capacity,
            entries: new_entries,
            bounds: new_bounds,
            value: new_value,
        }
    }

    pub fn pop(&mut self, monitor: &MonitorHandle) -> Option<Job> {
        monitor.inc_popped_urls();
        self.entries.iter_mut()
            .filter(|(_, _, popped)| !popped)
            .max_by_key(|(_, count, _)| *count)
            .map(|(url, count, popped)| {
                *popped = true;
                (url.clone(), *count)
            })
            .map(|(url, priority)| {
                self.value = Self::compute_value(&self.entries);
                Job::with_priority(url, priority)
            })
    }

    fn compute_value(entries: &[(String, u32, bool)]) -> u32 {
        entries.iter()
            .filter(|(_, _, popped)| !*popped)
            .map(|(_, count, _)| *count)
            .max()
            .unwrap_or(0)
    }

    pub fn recompute_value(&self) -> u32 {
        Self::compute_value(&self.entries)
    }

    pub fn serialize(self, config: &Config) -> Vec<u8> {
        // let mut bytes = Vec::with_capacity(4 + (2 + config.max_url_len) * 2 + (1 + 4 + 1 + config.max_url_len) * self.entries.len());
        let mut bytes = Vec::with_capacity(config.page_size_bytes);
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
        bytes.resize(config.page_size_bytes, 0);
        bytes
    }

    pub fn deserialize(config: &Config, bytes: Vec<u8>) -> Page {
        let n_entries = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let bounds_len = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let bounds = PageBounds::deserialize(&bytes[8..(8 + bounds_len)]);
        // println!("deserialized bounds {:?}", bounds);
        let mut i = 8 + bounds_len;
        let mut entries = Vec::with_capacity(n_entries);
        for _ in 0..n_entries {
            let url_len = bytes[i] as usize;
            i += 1;
            let count = u32::from_be_bytes(bytes[i..(i + 4)].try_into().unwrap());
            i += 4;
            let popped = bytes[i] > 0;
            i += 1;
            let url = String::from_utf8(bytes[i..(i + url_len)].to_vec()).unwrap();
            i += url_len;
            entries.push((url, count, popped));
        }
        let value = Self::compute_value(&entries);
        Page {
            value,
            entries,
            bounds,
            capacity: config.page_capacity,
        }
    }
}
