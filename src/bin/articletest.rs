use lazy_static::lazy_static;
use reqwest::Client;
use std::error::Error;
use regex::Regex;

lazy_static! {
    static ref BODY_RE: Regex = Regex::new(r"(?s)<body[^<>]*>.*(</body>|<script>)?").unwrap();
    static ref TAG_TEXT_RE: Regex = Regex::new(r">([^<>]+)").unwrap();
    static ref TERM_RE: Regex = Regex::new(r"[a-zA-Z]+").unwrap();
}


fn index_document(document: &str) {
    let mut n_terms = 0;
    let body = BODY_RE.find(document).unwrap().as_str();
    println!("body: {}", body);
    for tag_text in TAG_TEXT_RE.captures_iter(body) {
        println!("tag_text: {}", &tag_text[1]);
        for _term in TERM_RE.find_iter(&tag_text[1]) {
            n_terms += 1;
        }
    }
    println!("n terms: {}", n_terms);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = "https://scratch.mit.edu/projects/408075077";
    let client = Client::builder()
        .user_agent("Rustbot/0.2")                
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build().unwrap();

    let content = client.get(url).send().await?.text().await?;
    println!("got content of length {}", content.len());
    index_document(&content);

    // let start = Instant::now();
    // for _ in 0..1000 {
    //     // println!("iter {}", i);
    //     index_document(&content);
    // }
    // println!("time per iteration: {:?}", start.elapsed() / 1000);
    
    Ok(())
}
