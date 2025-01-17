use crate::pqueueevent::PQueueEvent;
use crate::page::Page;
use crate::config::Config;

use std::fs;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::thread;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::collections::BTreeMap;

pub enum DiskThreadEvent {
    Read(usize),
    Write(usize, Page),
}

pub struct DiskPQueueThread {
    config: Config,
    pqueue_sender: Sender<PQueueEvent>,
    file: fs::File,
    file_len: u64,
    offsets: BTreeMap<usize, u64>,
}

impl DiskPQueueThread {
    pub fn spawn(pqueue_id: usize, tid: usize, config: Config, pqueue_sender: Sender<PQueueEvent>) -> Sender<DiskThreadEvent> {
        let (event_sender, event_receiver) = channel();
        let dir = config.pqueue_path
            .join(format!("pqueue{}", pqueue_id));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("shard{}", tid));
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut pqueue_thread = DiskPQueueThread {
            config,
            file,
            pqueue_sender,
            file_len: 0,
            offsets: BTreeMap::new(),
        };
        thread::spawn(move || pqueue_thread.run(event_receiver));
        event_sender
    }

    fn run(&mut self, event_receiver: Receiver<DiskThreadEvent>) {
        for event in event_receiver {
            match event {
                DiskThreadEvent::Read(id) => self.read_page(id),
                DiskThreadEvent::Write(id, page) => self.write_page(id, page),
            }
        }
    }

    fn read_page(&mut self, id: usize) {
        let offset = &self.offsets[&id];
        let mut bytes = vec![0 as u8; self.config.page_size_bytes];
        self.file.seek(SeekFrom::Start(*offset)).unwrap();
        // println!("reading page {} from offset {}", id, *offset);
        self.file.read_exact(&mut bytes).unwrap();
        let page = Page::deserialize(&self.config, bytes);
        self.pqueue_sender.send(PQueueEvent::ReadResponse(id, page)).unwrap();
    }

    fn write_page(&mut self, id: usize, page: Page) {
        match self.offsets.get(&id) {
            Some(&offset) => self.write_to_offset(offset, page),
            None => {
                let offset = self.file_len;
                // println!("writing page {} to offset {}", id, offset);
                self.write_to_offset(offset, page);
                self.file_len += self.config.page_size_bytes as u64;
                self.offsets.insert(id, offset);
            },
        }
    }

    fn write_to_offset(&mut self, offset: u64, page: Page) {
        let bytes = page.serialize(&self.config);
        assert!(bytes.len() == self.config.page_size_bytes);
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.write_all(&bytes).unwrap();
        self.file.sync_all().unwrap();
    }
}
