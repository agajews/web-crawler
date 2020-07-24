pub struct Client {
    client: reqwest::Client,
    ip: IpAddr,
    request_count: usize,
    refresh_interval: usize,
}

impl Client {
    pub fn new(refresh_interval: usize) -> Client {
        let ips = datalink::interfaces()
            .into_iter()
            .filter(|interface| !interface.is_loopback())
            .map(|interface| interface.ips)
            .flatten()
            .filter(|ip| ip.is_ipv4())
            .collect::<Vec<_>>();
        let rng = rand::thread_rng();
        let ip = ips[rng.gen::<usize>() % ips.len()].ip();
        let client = Self::build_client(&ip);
        Client {
            client,
            ip,
            refresh_interval,
            request_count: 0,
        }
    }

    pub async fn get(&mut self, url: String) -> reqwest::Response {
        self.request_count += 1;
        if request_count % self.refresh_interval {
            self.refresh();
        }
        self.client.get(url).send().await
    }

    fn build_client(ip: &IpAddr) -> request::Client {
        reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .redirect(Policy::limited(100))
            .timeout(Duration::from_secs(60))
            .local_address(ip)
            .build().unwrap()
    }

    fn refresh(&mut self) {
        self.client = Self::build_client(self.ip);
    }

    fn read_capped_bytes(res: reqwest::Response, n_bytes: usize) -> Vec<u8> {
        let mut stream = res.bytes_stream();
        let mut total_len: usize = 0;
        let mut data = Vec::new();
        while let Some(Ok(chunk)) = stream.next().await {
            total_len += chunk.len();
            if total_len > n_bytes {
                break;
            }
            data.extend_from_slice(&chunk);
        }
        data
    }
}
