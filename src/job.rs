use crate::pagebounds::PageBoundsCmp;

use url::Url;

pub type JobLocality = String;

#[derive(PartialEq, Eq, Debug)]
pub struct Job {
    pub url: String,
}

impl Job {
    pub fn new(url: String) -> Job {
        Job { url }
    }

    pub fn locality(&self) -> JobLocality {
        String::from(Url::parse(&self.url).unwrap().host_str().unwrap())
    }

    pub fn cmp_ref(&self) -> PageBoundsCmp {
        PageBoundsCmp::Value(self.url.clone())
    }
}
