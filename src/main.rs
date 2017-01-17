#![feature(plugin)]
#![plugin(clippy)]

#[macro_use] extern crate tarpc;
#[macro_use] extern crate serde_derive;
extern crate time;
extern crate docopt;
extern crate rand;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate timer;

mod rpc;
mod node;
mod raft_node;
mod raft_server;
mod rpc_server;
mod event;
mod entry_log;
mod store;

use docopt::Docopt;
use rpc::ServerId;

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


struct Store;

impl store::Store for Store {
    fn apply(&mut self, entry: &rpc::Entry) {
    }
}


fn main() {
    env_logger::init().unwrap();
    let args = Docopt::new(USAGE)
        .and_then(|d| d.parse())
        .unwrap_or_else(|e| e.exit());
    let port = args.get_str("--listen");
    let addr = format!("localhost:{}", port);
    let mut servers = args.get_vec("--peers");
    servers.push(addr.as_str());
    let server_ids = servers.iter().map(|s| ServerId(s.to_string())).collect();
    let server = raft_server::RaftServer::new(ServerId(addr.to_owned()), Store, server_ids);
    server.run_forever();
}
