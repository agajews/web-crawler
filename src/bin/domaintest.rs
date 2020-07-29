
fn domain_root(domain: &str) -> String {
    let subdomains = domain.split('.').collect::<Vec<_>>();
    subdomains[(subdomains.len() - 2)..].join(".")
}

fn main() {
    println!("{}", domain_root("columbia.edu"));
    println!("{}", domain_root("math.columbia.edu"));
    println!("{}", domain_root("src.math.columbia.edu"));
}
