use crate::monitor::MonitorHandle;
use crate::client::Client;
use crate::config::Config;

use std::sync::Mutex;
use regex::Regex;
use std::collections::BTreeMap;
use url::Url;


pub struct RobotsChecker {
    config: Config,
    disallow_re: Regex,
    cache: Mutex<BTreeMap<String, Option<Vec<String>>>>,
    monitor: MonitorHandle,
}

impl RobotsChecker {
    pub fn new(config: Config, monitor: MonitorHandle) -> RobotsChecker {
        RobotsChecker {
            config,
            monitor,
            cache: Mutex::new(BTreeMap::new()),
            disallow_re: Regex::new(r"^Disallow: ([^\s]+)").unwrap(),
        }
    }

    pub async fn allowed(&self, url: &Url, client: &mut Client) -> bool {
        let host = url.host_str().unwrap();
        {
            let cache = self.cache.lock().unwrap();
            self.monitor.inc_robots_queries();
            if let Some(prefixes) = cache.get(host) {
                self.monitor.inc_robots_hits();
                return Self::match_url(url, prefixes);
            }
        }
        let prefixes = self.fetch_robots(url, client).await;
        let allowed = Self::match_url(url, &prefixes);
        let mut cache = self.cache.lock().unwrap();
        cache.insert(String::from(host), prefixes);
        allowed
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

    async fn fetch_robots(&self, url: &Url, client: &mut Client) -> Option<Vec<String>> {
        let res = client.get(url.join("/robots.txt").ok()?).await.ok()?;
        let robots = Client::read_capped_bytes(res, self.config.max_document_len).await;
        let robots = String::from_utf8_lossy(&robots);
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
