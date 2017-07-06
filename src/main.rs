#![feature(plugin)]

#[macro_use]
extern crate tarpc;
#[macro_use]
extern crate serde_derive;
extern crate time;
extern crate docopt;
extern crate rand;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate timer;
extern crate crossbeam;

mod rpc;
mod node;
mod raft_node;
mod rpc_server;
mod entry_log;
mod store;
mod clock;
mod request;
mod connection_pool;

use docopt::Docopt;
use rpc::{ServerId, Service};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;
use raft_node::RaftStore;

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
    fn apply(&mut self, entry: &rpc::Entry) {}
}


fn main() {
    log4rs::init_file("log4rs.yml", Default::default()).unwrap();
    let args = Docopt::new(USAGE)
        .and_then(|d| d.parse())
        .unwrap_or_else(|e| e.exit());
    let port = args.get_str("--listen");
    let addr = format!("localhost:{}", port);
    let mut servers = args.get_vec("--peers");
    servers.push(addr.as_str());
    let server_id = ServerId(addr.clone());
    let server_ids: Vec<ServerId> = servers.iter().map(|s| ServerId(s.to_string())).collect();

    let (sender, receiver) = channel::<request::Request>();
    let arc_sender = Arc::new(Mutex::new(sender));

    let sender2 = arc_sender.clone();
    let handler = thread::Builder::new()
        .name("eventloop".into())
        .spawn(move || {
            let pool = Mutex::new(connection_pool::ConnectionPool::new(server_ids.clone()));
            let mut store = RaftStore::new(server_id, Store, server_ids, sender2);

            loop {
                use request::*;
                use raft_node::LiveRaftStore;
                let beat = receiver.recv().expect("recv");

                match beat {
                    Request::TickRequest(req) => {
                        store.on_clock_tick();
                        let _ = req.ret.send(());
                    },
                    Request::VoteRequest(vote_req) => {
                        let resp = store.on_request_vote(&vote_req.data);
                        let _ = vote_req.ret.send(resp);
                    },
                    Request::AppendEntriesRequest(append_req) => {
                        let resp = store.on_append_entries(&append_req.data);
                        let _ = append_req.ret.send(resp);
                    },
                    Request::VoteFor(server_id, req) => {
                        crossbeam::scope(|scope| {
                            scope.spawn(|| {
                                let mut rpool = pool.lock().unwrap();
                                let ret = rpool.get_client(&server_id).and_then(|c| {
                                    c.on_request_vote(req).ok()
                                }).map(|resp| {
                                    store.on_receive_vote_request(&server_id, resp)
                                });
                                if ret.is_none() {
                                    rpool.remove_client(&server_id);
                                }
                            });
                        });
                    },
                    Request::AppendEntriesFor(server_id, req) => {
                        info!("append req: {:?} {:?}", server_id, req);
                        let is_heartbeat = req.entries.is_empty();
                        crossbeam::scope(|scope| {
                            scope.spawn(|| {
                                let mut rpool = pool.lock().unwrap();
                                let ret = rpool.get_client(&server_id).and_then(|c| {
                                    c.on_append_entries(req).ok()
                                }).map(|resp| {
                                    if is_heartbeat {
                                        store.on_receive_heartbeat(&server_id, resp)
                                    } else {
                                        store.on_receive_append_entries_request(&server_id, resp)
                                    }
                                });
                                if ret.is_none() {
                                    error!("remove client {}", &server_id);
                                    rpool.remove_client(&server_id);
                                }
                            });
                        });
                    }
                }
            }
        }).expect("unwrap eventloop thread");

    let s = rpc_server::RpcServer::new(arc_sender.clone());
    let _server_handle = s.spawn_with_config(addr.as_str(), tarpc::Config {
        timeout: Some(Duration::new(5, 0))
    }).expect("listen");

    let clock_sender = arc_sender.clone();
    let wall_clock = clock::Clock::new(clock_sender);
    let timer = timer::Timer::new();
    let _guard = timer.schedule_repeating(time::Duration::milliseconds(100), move || {
        wall_clock.tick();
    });

    handler.join().expect("join eventloop");
}
