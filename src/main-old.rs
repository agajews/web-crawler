extern crate serde;

use rayon::prelude::*;
use lazy_static::lazy_static;
use std::error::Error;
use reqwest::blocking::Client;
use select::document::Document;
use select::predicate::Name;
use regex::Regex;
use url::Url;
use std::collections::BTreeMap;
use bincode::{serialize, deserialize};
use std::time::{Instant, Duration};
use std::env;
use web_index::{Meta, Index};
use rand;
use std::path::PathBuf;
use std::fs;
use uuid::Uuid;
use rand::seq::SliceRandom;
use std::thread::sleep;
use std::fs::OpenOptions;
use hex;

// variables to set:
// URL_BLOCK_DIR - directory for pending url blocks
// URL_CLAIMED_BLOCK_DIR - directory for claimed url blocks
// SEEN_URL_DIR - path to directory of seen urls
// META_DIR - directory of metadata databases
// INDEX_DIR - directory of document indexes

const URL_BLOCK_SIZE: usize = 100;

lazy_static! {
    static ref ACADEMIC_RE: Regex = Regex::new(r".+\.(edu|ac\.??)").unwrap();
}

fn is_academic(url: &Url) -> bool {
    match url.domain() {
        Some(domain) => ACADEMIC_RE.is_match(domain),
        None => false,
    }
}

fn tokenize(document: &Document) -> Vec<String> {
    let text = match document.find(Name("body")).next() {
        Some(body) => body.text(),
        None => String::from(""),
    };
    let mut tokens: Vec<String> = text.split_whitespace()
        .map(String::from)
        .collect();

    for token in &mut tokens {
        token.retain(|c| c.is_ascii_alphabetic());
        token.make_ascii_lowercase();
    }

    tokens
        .into_iter()
        .filter(|t| t.len() > 0)
        .collect()
}

fn count_terms(document: &Document) -> Vec<(String, u32)> {
    let mut terms: BTreeMap<String, u32> = BTreeMap::new();
    for token in tokenize(document) {
        let count = match terms.get(&token) {
            Some(count) => count.clone(),
            None => 0,
        };
        terms.insert(token, count + 1);
    }
    terms.into_iter().collect()
}

struct DocStats {
    id: u32,
    url: String,
    terms: Vec<(String, u32)>,
    n_terms: u32,
    links: Option<Vec<String>>,
}

lazy_static! {
    static ref SEEN_DIR: PathBuf = PathBuf::from(env::var("SEEN_URL_DIR").unwrap());
    static ref META: Meta = Meta::new(PathBuf::from(env::var("META_DIR").unwrap()).join(Uuid::new_v4().to_string()));
    static ref INDEX: Index = Index::new(PathBuf::from(env::var("INDEX_DIR").unwrap()).join(Uuid::new_v4().to_string()));
}

fn crawl(id: u32, url: String, client: Client) -> Option<DocStats> {
    let res = client.get(&url)
        .send().ok()?
        .text().ok()?;

    let document = Document::from(res.as_str());
    let terms = count_terms(&document);
    let n_terms = terms
        .iter()
        .map(|(_term, count)| count)
        .sum();

    let source_url = Url::parse(&url).ok()?;
    if !is_academic(&source_url) {
        return Some(DocStats {id, url, terms, n_terms, links: None})
    }

    let links = document
        .find(Name("a"))
        .filter_map(|node| node.attr("href"))
        .filter_map(|href| source_url.join(href).ok())
        .filter(|href| href.scheme().starts_with("http"))
        .map(Url::into_string)
        .collect();

    return Some(DocStats {id, url, terms, n_terms, links: Some(links)})
}

fn crawl_block(urls: Vec<String>, start_id: u32) {
    let client = Client::builder()
        .user_agent("Rustbot/0.1")
        .build().unwrap();

    let mut args = Vec::new();
    for (i, url) in urls.into_iter().enumerate() {
        args.push((start_id + i as u32, url, client.clone()));
    }

    let document_stats: Vec<DocStats> = args
        .into_par_iter()
        .map(|(id, url, client)| match crawl(id, url.clone(), client) {
            Some(stats) => Some(stats),
            None => { println!("error crawling {}", url); None },
        })
        .filter_map(|x| x)
        .collect();

    document_stats.iter()
        .filter_map(|stats| stats.links.clone())
        .flatten()
        .filter(|link| try_claim(link))
        .collect::<Vec<String>>()
        .chunks(URL_BLOCK_SIZE)
        .map(Vec::from)
        .for_each(write_block);

    for stats in &document_stats {
        META.insert(stats.id, (stats.n_terms, stats.url.clone()));
    }

    for stats in document_stats {
        for (term, count) in stats.terms {
            INDEX.insert(&term, (stats.id, count));
        }
    }
}

fn write_block(data: Vec<String>) {
    let block_dir = PathBuf::from(env::var("URL_BLOCK_DIR").unwrap());
    let filename = block_dir.join(Uuid::new_v4().to_string());
    fs::write(filename, serialize(&data).unwrap()).unwrap();
}

fn claim_block() -> Vec<String> {
    let block_dir = PathBuf::from(env::var("URL_BLOCK_DIR").unwrap());
    let claimed_block_dir = PathBuf::from(env::var("URL_CLAIMED_BLOCK_DIR").unwrap());
    let blocks: Vec<PathBuf> = fs::read_dir(block_dir)
        .unwrap()
        .map(|res| res.unwrap().path())
        .collect();
    if blocks.is_empty() {
        println!("no blocks to claim, waiting 1s...");
        sleep(Duration::from_millis(1000));
        return claim_block();
    }
    let block = blocks.choose(&mut rand::thread_rng()).unwrap();
    let claimed_block = claimed_block_dir.join(Uuid::new_v4().to_string());
    if let Err(_) = fs::rename(block, &claimed_block) {
        return claim_block();  // try again
    }
    let data = fs::read(claimed_block).unwrap();
    deserialize(&data).unwrap()
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut id = 0;
    let root_urls: Vec<String> = {
        vec!["https://columbia.edu", "https://harvard.edu", "https://stanford.edu", "https://www.cam.ac.uk"]
            .into_iter()
            .filter(|url| try_claim(url))
            .map(String::from)
            .collect()
    };

    let n_root_urls = root_urls.len() as u32;
    let start = Instant::now();
    crawl_block(root_urls, id);
    id += n_root_urls;
    println!("finished root block in {:?}", start.elapsed());

    loop {
        let block = claim_block();
        println!("starting to work on new block");
        let start = Instant::now();
        let block_size = block.len() as u32;
        crawl_block(block, id);
        id += block_size;
        println!("finished block in {:?}, at id {}", start.elapsed(), id);
    }
}
