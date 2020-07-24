
DiskPQueueThread::spawn(
    page_request_receiver,
    page_sender,
    page_write_request_receiver,
    page_write_sender,
);

struct DiskPQueueThread {
    config: Config,
    event_receiver: Receiver<DiskThreadEvent>,
    pqueue_sender: Sender<PQueueEvent>,
    file: fs::File,
    file_len: u64,
    offsets: BTreeMap<usize, u64>,
}

impl DiskPQueueThread {
    fn spawn(tid: usize, config: Config, pqueue_sender: Sender<PQueueEvent>) -> Sender<DiskThreadEvent> {
        let (event_sender, event_receiver) = channel();
        let path = config.pqueue_path.join(format!("shard{}", tid));
        let pqueue_thread = DiskPQueueThread {
            config,
            event_receiver,
            pqueue_sender,
            file_len: 0,
            file: fs::File::create(path),
        };
        thread::spawn(move || pqueue_thread.run());
        event_sender
    }

    fn run(&mut self) {
        for event in self.event_receiver {
            match event {
                DiskThreadEvent::Read(id) => self.read_page(id),
                DiskThreadEvent::Write(id, page) => self.write_page(id, page),
            }
        }
    }

    fn read_page(&mut self, id: usize) {
        let offset = &self.offsets[id];
        let bytes = vec![0 as u8; self.config.page_size_bytes];
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.read_exact(&mut bytes).unwrap();
        let page = Page::deserialize(&self.config, bytes);
        self.pqueue_sender.send(PQueueEvent::ReadResponse(id, page)).unwrap();
    }

    fn write_page(&mut self, id: usize, page: Page) {
        match self.offsets.get(id) {
            Some(offset) => self.write_to_offset(offset, page),
            None => {
                let offset = self.file_len;
                self.write_to_offset(offset, page);
                self.file_len += self.config.page_size_bytes;
                self.offsets.insert(id, offset);
            },
        }
        self.pqueue_sender.send(PQueueEvent::WriteResponse(id)).unwrap();
    }

    fn write_to_offset(&mut self, offset: u64, page: Page) {
        let bytes = page.serialize(&self.config);
        assert!(bytes.len() <= self.config.page_size_bytes);
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.write_all(bytes);
        self.file.sync_all();
    }
}
