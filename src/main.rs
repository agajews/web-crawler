use rayon::prelude::*;
use lazy_static::lazy_static;
use std::error::Error;
use reqwest::blocking::Client;
use select::document::Document;
use select::predicate::Name;
use regex::Regex;
use url::Url;
use std::collections::BTreeMap;

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
        token.retain(|s| s.is_ascii());
        token.make_ascii_lowercase();
    }

    tokens
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
    terms: Vec<(String, u32)>,
    n_terms: u32,
    links: Option<Vec<String>>,
}

fn crawl(url: String, id: u32) -> Option<DocStats> {
    let client = Client::builder()
        .user_agent("Rustbot/0.1")
        .build().unwrap();
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
        return Some(DocStats {id, terms, n_terms, links: None})
    }

    let links = document
        .find(Name("a"))
        .filter_map(|node| node.attr("href"))
        .filter_map(|href| source_url.join(href).ok())
        .filter(|href| href.scheme().starts_with("http"))
        .map(Url::into_string)
        .collect();

    return Some(DocStats {id, terms, n_terms, links: Some(links)})
}

fn crawl_block(urls: Vec<String>, ids: Vec<u32>) -> Vec<String> {
    let urls_and_ids: Vec<(String, u32)> = urls
        .into_iter()
        .zip(ids)
        .collect();

    let mut document_stats: Vec<DocStats> = urls_and_ids
        .into_par_iter()
        .map(|(url, id)| crawl(url, id))
        .filter_map(|x| x)
        .collect();

    let mut links = Vec::new();
    for stats in &mut document_stats {
        if let Some(doc_links) = &mut stats.links {
            links.append(doc_links);
        }
    }

    for link in &links {
        println!("{}", link);
    }

    let mut docmeta: BTreeMap<u32, u32> = BTreeMap::new();
    for stats in &document_stats {
        docmeta.insert(stats.id, stats.n_terms);
    }

    let mut index: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
    for stats in document_stats {
        for (term, count) in stats.terms {
            if let Some(postings) = index.get_mut(&term) {
                postings.push((stats.id, count));
            } else {
                index.insert(term, vec![(stats.id, count)]);
            }
        }
    }

    links
}

fn main() -> Result<(), Box<dyn Error>> {
    let ids = (0..4).collect();
    let urls: Vec<String> = vec!["https://columbia.edu", "https://harvard.edu", "https://stanford.edu", "https://www.cam.ac.uk"]
        .into_iter()
        .map(String::from)
        .collect();

    let links = crawl_block(urls, ids);

    let ids = (4..(links.len() as u32 + 4)).collect();
    let _links = crawl_block(links, ids);

    Ok(())
}
