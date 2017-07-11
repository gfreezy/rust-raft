#![feature(plugin)]
#![plugin(rocket_codegen)]
#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate rocket;
extern crate rocket_contrib;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate time;
extern crate docopt;
extern crate rand;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate timer;
extern crate futures;
extern crate hyper;
extern crate tokio_core;

mod rpc;
mod node;
mod raft_node;
mod rpc_server;
mod entry_log;
mod store;
mod clock;
mod request;
mod connection_pool;
mod errors;

use docopt::Docopt;
use rpc::ServerId;
use std::sync::mpsc::channel;
use std::thread;
use raft_node::RaftStore;
use request::Request;
use std::collections::HashMap;

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


const ZERO_INDEX: u64 = 0;
const NEXT_DATA_INDEX: u64 = 2;

struct Store(HashMap<String, String>);

impl store::Store for Store {
    fn apply(&mut self, entry: &rpc::Entry) {
        self.0.insert(entry.cmd.clone(), entry.payload.clone());
        println!("cmd: {}, payload: {}", entry.cmd, entry.payload);
    }
}


fn main() {
    log4rs::init_file("log4rs.yml", Default::default()).unwrap();
    let args = Docopt::new(USAGE)
        .and_then(|d| d.parse())
        .unwrap_or_else(|e| e.exit());
    let port: u16 = args.get_str("--listen").parse().unwrap();
    let addr = format!("localhost:{}", port);
    let mut servers = args.get_vec("--peers");
    servers.push(addr.as_str());
    let server_id = ServerId(addr.clone());
    let server_ids: Vec<ServerId> = servers.iter().map(|s| ServerId(s.to_string())).collect();

    let (sender, receiver) = channel::<Request>();

    let sender2 = sender.clone();
    let handler = thread::Builder::new()
        .name("eventloop".into())
        .spawn(move || {
            let mut pool = connection_pool::ConnectionPool::new();
            let s = Store(HashMap::new());
            let mut store = RaftStore::new(server_id, s, server_ids, sender2);

            loop {
                use raft_node::LiveRaftStore;
                let beat = receiver.recv().expect("recv");

                match beat {
                    Request::TickRequest(req) => {
                        store.on_clock_tick();
                        let _ = req.ret.send(());
                    }
                    Request::VoteRequest(vote_req) => {
                        let resp = store.on_request_vote(&vote_req.data);
                        let _ = vote_req.ret.send(resp);
                    }
                    Request::AppendEntriesRequest(append_req) => {
                        let resp = store.on_append_entries(&append_req.data);
                        let _ = append_req.ret.send(resp);
                    }
                    Request::CommandRequest(command_req) => {
                        let resp = store.on_receive_command(command_req.data);
                        let _ = command_req.ret.send(resp);
                    }
                    Request::VoteFor(reqs) => {
                        let _ = pool.on_request_vote(reqs).map(|resps| {
                            for (server_id, req, resp) in resps {
                                store.on_receive_vote_request(&server_id, req, resp)
                            }
                        });
                    }
                    Request::AppendEntriesFor(reqs) => {
                        let _ = pool.on_append_entries(reqs).map(|resps| {
                            for (server_id, req, resp) in resps {
                                store.on_receive_append_entries_request(&server_id, req, resp)
                            }
                        });
                    }
                }
            }
        }).expect("unwrap eventloop thread");

    let sender3 = sender.clone();
    let _ = thread::Builder::new()
        .name("rocket-server".into())
        .spawn(move || {
            rpc_server::start_rocket(sender3, port).launch();
        });

    let wall_clock = clock::Clock::new(sender);
    let timer = timer::Timer::new();
    let _guard = timer.schedule_repeating(time::Duration::milliseconds(100), move || {
        wall_clock.tick();
    });

    handler.join().expect("join eventloop");
}
