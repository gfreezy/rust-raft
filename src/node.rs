use std::collections::HashSet;
use std::sync::mpsc::Sender;

use rpc::{AppendEntriesReq, AppendEntriesResp, EntryIndex, ServerId, Term, VoteReq, VoteResp};
use entry_log::EntryLog;
use store::Store;
use request::Request;

// 500ms timeout
pub const ELECTION_TIMEOUT: u64 = 1000;
pub const HEARTBEAT_INTERVAL: u64 = 100;

pub trait NodeListener {
    fn on_append_entries(&mut self, req: AppendEntriesReq, remote_server: ServerId, ) -> (AppendEntriesResp, bool);
    fn on_request_vote(&mut self, req: VoteReq, remote_server: ServerId) -> (VoteResp, bool);
    fn on_clock_tick(&mut self) -> bool;
}

#[derive(Debug)]
pub struct Node<T, S: Store> {
    pub server_id: ServerId,
    pub current_term: Term,
    pub voted_for: Option<ServerId>,
    pub log: EntryLog,
    pub commit_index: EntryIndex,
    pub last_applied_id: EntryIndex,
    pub noti_center: Sender<Request>,
    pub servers: HashSet<ServerId>,
    pub new_servers: HashSet<ServerId>,
    pub non_voting_servers: HashSet<ServerId>,
    pub state: T,
    pub store: S,
}

impl<T, S: Store> Node<T, S> {
    pub fn trigger(&self, req: Request) {
        self.noti_center.send(req).expect("send event");
    }

    pub fn apply_log(&mut self) {
        while self.commit_index > self.last_applied_id {
            self.last_applied_id.incr();
            let entry = self.log.get(self.last_applied_id);
            if let Some(e) = entry {
                self.store.apply(e)
            }
        }
    }

    pub fn peers(&self) -> Vec<ServerId> {
        self.servers
            .iter()
            .filter(|server| **server != self.server_id)
            .cloned()
            .collect::<Vec<ServerId>>()
    }
}
