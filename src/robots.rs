use crate::monitor::MonitorHandle;
use crate::client::Client;

use std::sync::Mutex;
use regex::Regex;
use std::collections::BTreeMap;


pub struct RobotsChecker {
    disallow_re: Regex,
    cache: Mutex<BTreeMap<String, Option<Vec<String>>>>,
    monitor: MonitorHandle,
}

impl RobotsChecker {
    pub fn new(monitor: MonitorHandle) -> RobotsChecker {
        RobotsChecker {
            monitor,
            cache: Mutex::new(BTreeMap::new()),
            disallow_re: Regex::new(r"^Disallow: ([^\s]+)").unwrap(),
        }
    }

    pub async fn allowed(&self, url: &Url, client: &Client) -> bool {
        let host = match url.host_str().unwrap();
        let cache = self.cache.lock().unwrap();
        let prefixes = match cache.get(host) {
            Some(prefixes) => {
                self.monitor.inc_robots_hits();
                prefixes
            },
            None => {
                let prefixes = self.fetch_robots(url, client).await;
                cache.insert(String::from(host), prefixes);
                &cache[host]
            },
        };
        match_url(url, prefixes)
    }

    fn match_url(url: &Url, prefixes: &Option<Vec<String>>) -> bool {
        let prefixes = match prefixes {
            Some(prefixes) => prefixes,
            None => return true,
        };
        let path = url.path();
        for prefix in prefixes {
            if path.starts_with(prefix) {
                return false;
            }
        }
        return true;
    }

    async fn fetch_robots(url: &Url, client: &Client) -> Option<Vec<String>> {
        let res = client.get(url.join("/robots.txt").ok()?).await.ok()?;
        let robots = Client::read_capped_bytes(res, self.config.max_document_len);
        let robots = String::from_utf8_lossy(robots);
        let mut prefixes = Vec::new();
        let mut should_match = false;
        for line in robots.lines() {
            if line.starts_with("User-agent: ") {
                should_match = line.starts_with("User-agent: *") || line.starts_with("User-agent: Rustbot");
                continue;
            }
            if should_match {
                match self.disallow_re.captures(line) {
                    Some(caps) => match caps.get(1) {
                        Some(prefix) => prefixes.push(String::from(prefix.as_str())),
                        None => continue,
                    },
                    None => continue,
                }
            }
        }
        if prefixes.is_empty() {
            return None;
        }
        Some(prefixes)
    }
}
