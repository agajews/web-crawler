use reqwest;
use reqwest::redirect::Policy;
use std::net::IpAddr;
use pnet::datalink;
use rand;
use rand::prelude::*;
use std::time::Duration;
use std::error::Error;
use tokio::stream::StreamExt;

pub struct Client {
    client: reqwest::Client,
    ip: IpAddr,
    user_agent: String,
    request_count: usize,
    refresh_interval: usize,
}

impl Client {
    pub fn new(user_agent: String, refresh_interval: usize) -> Client {
        let ips = datalink::interfaces()
            .into_iter()
            .filter(|interface| !interface.is_loopback())
            .map(|interface| interface.ips)
            .flatten()
            .filter(|ip| ip.is_ipv4())
            .collect::<Vec<_>>();
        let ip = ips[rand::thread_rng().gen::<usize>() % ips.len()].ip();
        let client = Self::build_client(&user_agent, ip.clone());
        Client {
            client,
            ip,
            user_agent,
            refresh_interval,
            request_count: 0,
        }
    }

    pub async fn get(&mut self, url: String) -> Result<reqwest::Response, Box<dyn Error>> {
        self.request_count += 1;
        if self.request_count % self.refresh_interval == 0 {
            self.refresh();
        }
        Ok(self.client.get(&url).send().await?)
    }

    fn build_client(user_agent: &str, ip: IpAddr) -> reqwest::Client {
        reqwest::Client::builder()
            .user_agent(user_agent)
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .redirect(Policy::limited(100))
            .timeout(Duration::from_secs(60))
            .local_address(ip)
            .build().unwrap()
    }

    fn refresh(&mut self) {
        self.client = Self::build_client(&self.user_agent, self.ip.clone());
    }

    pub async fn read_capped_bytes(res: reqwest::Response, n_bytes: usize) -> Vec<u8> {
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
