#![feature(proc_macro)]
#![feature(conservative_impl_trait)]
#![feature(core)]

#[macro_use] extern crate tarpc;
#[macro_use] extern crate serde_derive;
extern crate time;

mod rpc;
mod node;
mod raft_server;
mod rpc_server;


use std::sync::Arc;
use ::rpc::Service;

fn main() {
    let servers = vec!["localhost:1111", "localhost:1112", "localhost:1113"];
    let addr = "localhost:1111";
    let mut server = Arc::new(raft_server::RaftServer::new(addr.to_string(), &servers));
    let s = rpc_server::RpcServer(server.clone());
    s.spawn(addr).unwrap();
    server.run_forever();
}
