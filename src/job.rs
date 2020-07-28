use crate::pagebounds::PageBoundsCmp;

use url::Url;

pub type JobLocality = String;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Job {
    pub url: String,
    pub priority: u32,
}

impl Job {
    pub fn new(url: String) -> Job {
        Job { url, priority: 0 }
    }

    pub fn with_priority(url: String, priority: u32) -> Job {
        Job { url, priority }
    }

    pub fn locality(&self) -> JobLocality {
        String::from(Url::parse(&self.url).unwrap().host_str().unwrap())
    }

    pub fn cmp_ref(&self) -> PageBoundsCmp {
        PageBoundsCmp::Value(self.url.clone())
    }
}
