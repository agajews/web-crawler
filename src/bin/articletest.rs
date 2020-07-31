use lazy_static::lazy_static;
use reqwest::Client;
use std::error::Error;
use regex::Regex;

lazy_static! {
    static ref BODY_RE: Regex = Regex::new(r"(?s)<(body|/script|/style)([^<>]*)>.*?(</body>|<script|<style)").unwrap();
    static ref TAG_TEXT_RE: Regex = Regex::new(r">([^<>]+)").unwrap();
    static ref TERM_RE: Regex = Regex::new(r"[a-zA-Z]+").unwrap();
}


fn index_document(document: &str) {
    let mut n_terms = 0;
    let mut n_article_terms = 0;
    for section in BODY_RE.find_iter(document) {
        // println!("body section: {}", section.as_str());
        for tag_text in TAG_TEXT_RE.captures_iter(section.as_str()) {
            println!("================================= tag section ===============================");
            let mut n_tag_terms = 0;
            for term in TERM_RE.find_iter(&tag_text[1]) {
                println!("{}", term.as_str());
                n_tag_terms += 1;
            }
            n_terms += n_tag_terms;
            if n_tag_terms > 30 {
                n_article_terms += n_tag_terms;
            }
        }
    }
    println!("n article terms: {}, total terms: {}", n_article_terms, n_terms);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = "http://www.catb.org/esr";
    // let url = "http://plus.google.com/108967323530519754654";
    // let url = "https://www.patreon.com/esr";
    // let url = "https://www.gunbroker.com";
    let client = Client::builder()
        .user_agent("Rustbot/0.2")                
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build().unwrap();

    let content = client.get(url).send().await?.text().await?;
    println!("got content of length {}", content.len());
    index_document(&content);

    Ok(())
}
