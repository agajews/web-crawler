pub struct Job {
    pub url: String,
}

pub type JobLocality = String;

impl Job {
    pub fn new(url: String) -> Job {
        Job { url }
    }

    pub fn locality(&self) -> JobLocality {
        Url::parse(self.url).unwrap().host_str().unwrap()
    }
}
