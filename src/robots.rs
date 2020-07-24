pub struct RobotsChecker {
    disallow_re: Regex,
    cache: Mutex<BTreeMap<String, Option<Vec<String>>>>,
}

impl RobotsChecker {
    pub async fn allowed(&self, url: &Url, client: &Client) -> bool {
        let host = match url.host_str().unwrap();
        let cache = self.cache.lock().unwrap();
        let prefixes = match cache.get(host) {
            Some(prefixes) => prefixes,
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
        let robots = client.read_capped_bytes(self.config.max_document_len);
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
