#![feature(proc_macro)]
#![feature(conservative_impl_trait)]
#![feature(core)]

#[macro_use] extern crate tarpc;
#[macro_use] extern crate serde_derive;
extern crate time;

macro_rules! inherit {
    {
        $(#[$flag_struct:meta])* struct $child:ident : $parent:ty {
            $($(#[$flag_field:meta])* $field:ident: $ty:ty),*
        }
    } => {
        extern crate core;

        $(#[$flag_struct])* struct $child {
            _super: $parent,
            $($(#[$flag_field])* $field: $ty),*
        }

        impl core::ops::Deref for $child {
            type Target = $parent;

            fn deref(&self) -> &$parent { &self._super }
        }

        impl core::ops::DerefMut for $child {
            fn deref_mut(&mut self) -> &mut $parent { &mut self._super }
        }
    }
}


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
