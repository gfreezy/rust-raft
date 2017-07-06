use std::collections::HashMap;
use std::time::Duration;
use tarpc;

use ::rpc::{ServerId, Client};

pub struct ConnectionPool {
    conns: HashMap<ServerId, tarpc::Result<Client>>,
}

impl ConnectionPool {
    pub fn new(server_ids: Vec<ServerId>) -> Self {
        let mut map = HashMap::new();
        for server_id in server_ids {
            let c = ConnectionPool::create_client(&server_id);
            map.insert(server_id, c);
        }
        ConnectionPool {
            conns: map,
        }
    }

    pub fn get_client(&mut self, server_id: &ServerId) -> Option<&Client> {
        let v = self.conns.entry(server_id.clone()).or_insert_with(|| Client::new(server_id.addr()));
        if v.is_ok() {
            return v.as_ref().ok();
        }
        let c = ConnectionPool::create_client(server_id);
        *v = c;
        v.as_ref().ok()
    }

    pub fn remove_client(&mut self, server_id: &ServerId) {
        self.conns.remove(server_id);
    }

    fn create_client(server_id: &ServerId) -> tarpc::Result<Client> {
        Client::with_config(server_id.addr(), tarpc::Config { timeout: None })
    }
}
