#![feature(proc_macro)]

#[macro_use] extern crate tarpc;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate chan;
extern crate time;
extern crate docopt;
extern crate rand;

use docopt::Docopt;

mod rpc;
mod node;
mod raft_server;
mod rpc_server;


const USAGE: &'static str = "
rust-raft.

Usage:
  rust-raft [-l <port>] --peers <peers>...
  rust-raft (-h | --help)
  rust-raft --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  -l, --listen=<port>  Listen on port [default: 1111].
  --peers=<peers>   Peers to connect to.
";


fn main() {
    let args = Docopt::new(USAGE)
        .and_then(|d| d.parse())
        .unwrap_or_else(|e| e.exit());
    let port = args.get_str("--listen");
    let addr = format!("localhost:{}", port);
    let mut servers = args.get_vec("--peers");
    servers.push(addr.as_str());
    let server = raft_server::RaftServer::new(addr.to_string(), &servers);
    server.run_forever();
}
