use url::Url;
use std::collections::BTreeMap;

fn looks_like_a_trap(url: &Url) -> bool {
    let segments = match url.path_segments() {
        Some(segments) => segments,
        None => return true,
    };
    let mut counts = BTreeMap::new();
    for segment in segments {
        let prev_count = counts.entry(segment)
            .or_insert(0);
        *prev_count += 1;
    }
    let n_dups: usize = counts.values()
        .map(|count| *count - 1)
        .sum();
    n_dups >= 2
}

fn main() {
    let url = Url::parse("https://give.stthomas.edu/about/gift-officers/about/impact/gratitude/about/impact/give/impact/morrison-family-college-of-health/index.html").unwrap();
    assert!(looks_like_a_trap(&url));
    let url = Url::parse("https://www.nap.edu/topic/404/behavioral-and-social-sciences/human-systems-and-technology").unwrap();
    assert!(!looks_like_a_trap(&url));
    let url = Url::parse("https://give.stthomas.edu/about/gift-officers/about/about/morrison-family-college-of-health/index.html").unwrap();
    assert!(looks_like_a_trap(&url));
}
