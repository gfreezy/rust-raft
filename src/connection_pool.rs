use std::collections::HashMap;
use tarpc;

use ::rpc::{ServerId, Client, VoteReq, VoteResp, AppendEntriesReq, AppendEntriesResp};

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

    fn get_client(&mut self, server_id: &ServerId) -> tarpc::Result<&Client> {
        let v = self.conns.entry(server_id.clone()).or_insert_with(|| Client::new(server_id.addr()));
        let ret = if v.is_ok() {
            v
        } else {
            let c = ConnectionPool::create_client(server_id);
            *v = c;
            v
        };
        ret.as_ref().map_err(|err| err.clone())
    }

    pub fn remove_client(&mut self, server_id: &ServerId) {
        self.conns.remove(server_id);
    }

    fn create_client(server_id: &ServerId) -> tarpc::Result<Client> {
        Client::with_config(server_id.addr(), tarpc::Config { timeout: None })
    }

    pub fn on_request_vote(&mut self, server_id: &ServerId, req: VoteReq) -> tarpc::Result<VoteResp> {
        let ret = self.get_client(server_id).and_then(|c| {
            c.on_request_vote(req)
        });
        if ret.is_err() {
            self.remove_client(server_id);
        }
        ret
    }

    pub fn on_append_entries(&mut self, server_id: &ServerId, req: AppendEntriesReq) -> tarpc::Result<AppendEntriesResp> {
        let ret = self.get_client(server_id).and_then(|c| {
            c.on_append_entries(req)
        });
        if ret.is_err() {
            self.remove_client(server_id);
        }
        ret
    }
}
