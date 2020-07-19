use std::{fs, env};
use rand::Rng;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::thread;
use std::thread::sleep;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::path::PathBuf;
use std::time::Duration;

const N_THREADS: usize = 32;

#[derive(Clone)]
struct Monitor {
    n_writes: Arc<AtomicUsize>,
}

impl Monitor {
    fn spawn() -> Monitor {
        let monitor = Monitor { n_writes: Arc::new(AtomicUsize::new(0)) };
        {
            let monitor = monitor.clone();
            thread::spawn(move || monitor.run());
        }
        monitor
    }

    fn inc_writes(&self) {
        self.n_writes.fetch_add(1, Ordering::Relaxed);
    }

    fn run(&self) {
        let mut prev_count = 0;
        loop {
            let curr_count = self.n_writes.load(Ordering::Relaxed);
            println!("{} writes/sec", curr_count - prev_count);
            prev_count = curr_count;
            sleep(Duration::from_secs(1));
        }
    }
}

fn bench_thread(path: PathBuf, monitor: Monitor) {
    let mut file = fs::File::create(path).unwrap();
    let n_bytes = 1_000_000_000;
    let bytes = vec![0 as u8; n_bytes];
    file.write_all(&bytes).unwrap();
    let mut rng = rand::thread_rng();

    loop {
        let i = rng.gen::<u64>() % n_bytes as u64;
        file.seek(SeekFrom::Start(i)).unwrap();
        file.write(&[1]).unwrap();
        file.sync_all().unwrap();
        monitor.inc_writes();
    }
}

fn main() {
    let monitor = Monitor::spawn();
    let dir: PathBuf = env::var("BENCH_DIR").unwrap().into();
    fs::create_dir_all(&dir).unwrap();

    let mut threads = Vec::new();
    for i in 0..N_THREADS {
        let path = dir.join(format!("{}", i));
        let monitor = monitor.clone();
        threads.push(thread::spawn(move || bench_thread(path, monitor)));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
