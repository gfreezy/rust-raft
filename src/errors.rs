error_chain! {
    foreign_links {
        Fmt(::std::fmt::Error);
        Io(::std::io::Error);
        Hyper(::hyper::Error);
        Serde(::serde_json::Error);
    }
}
