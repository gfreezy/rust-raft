use time;
use std;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;
use tarpc;
use std::cell::RefCell;
use ::rpc;


// 500ms timeout
const ELECTION_TIMEOUT: i64 = 1000;
const HEARTBEAT_INTERVAL: i64 = 100;


pub struct Node<T> {
    server_id: String,
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<rpc::Entry>,
    commit_index: u64,
    last_applied_id: u64,
    noti_center: Sender<Event>,
    servers: HashSet<String>,
    state: T,
}


pub struct Leader {
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
    heartbeat_sent_at: time::Tm,
}


pub struct Follower {
    heartbeat_received_at: time::Tm,
}

pub struct Candidate {
    votes: HashSet<String>,
    election_started_at: time::Tm,
}

pub enum Event {
    ConvertToFollower,
    ConvertToLeader,
    ConvertToCandidate,
    SendRequestVote((String, rpc::VoteReq)),
    SendAppendEntries((String, rpc::AppendEntriesReq)),
}


pub enum RaftNode {
    Leader(Node<Leader>),
    Follower(Node<Follower>),
    Candidate(Node<Candidate>),
}


impl RaftNode {
    pub fn new(server_id: String, servers: &[&str], noti_center: Sender<Event>) -> Self {
        RaftNode::Follower(Node::new(server_id, servers, noti_center))
    }
}


impl Node<Follower> {
    pub fn new(server_id: String, servers: &[&str], noti_center: Sender<Event>) -> Self {
        Node {
            server_id: server_id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied_id: 0,
            state: Follower {
                heartbeat_received_at: time::now_utc(),
            },
            noti_center: noti_center,
            servers: servers.iter().map(|&s| s.to_string()).collect(),
        }
    }
}


impl From<Node<Follower>> for Node<Candidate> {
    fn from(val: Node<Follower>) -> Self {
        let mut n: Node<Candidate> = Node {
            server_id: val.server_id.clone(),
            current_term: val.current_term,
            voted_for: Some(val.server_id),
            log: val.log,
            commit_index: val.commit_index,
            last_applied_id: val.last_applied_id,
            noti_center: val.noti_center,
            servers: val.servers,
            state: Candidate {
                votes: HashSet::new(),
                election_started_at: time::now_utc(),
            },
        };
        n.new_election();
        n
    }
}


impl From<Node<Candidate>> for Node<Follower> {
    fn from(val: Node<Candidate>) -> Self {
        Node {
            server_id: val.server_id,
            current_term: val.current_term,
            voted_for: None,
            log: val.log,
            commit_index: val.commit_index,
            last_applied_id: val.last_applied_id,
            noti_center: val.noti_center,
            servers: val.servers,
            state: Follower {
                heartbeat_received_at: time::now_utc(),
            },
        }
    }
}


impl From<Node<Candidate>> for Node<Leader> {
    fn from(val: Node<Candidate>) -> Self {
        let mut node = Node {
            server_id: val.server_id,
            current_term: val.current_term,
            voted_for: val.voted_for,
            log: val.log,
            commit_index: val.commit_index,
            last_applied_id: val.last_applied_id,
            noti_center: val.noti_center,
            state: Leader {
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                heartbeat_sent_at: time::now_utc(),
            },
            servers: val.servers,
        };
        node.send_heartbeat();
        node
    }
}


impl From<Node<Leader>> for Node<Follower> {
    fn from(val: Node<Leader>) -> Self {
        Node {
            server_id: val.server_id,
            current_term: val.current_term,
            voted_for: val.voted_for,
            log: val.log,
            commit_index: val.commit_index,
            last_applied_id: val.last_applied_id,
            noti_center: val.noti_center,
            servers: val.servers,
            state: Follower {
                heartbeat_received_at: time::now_utc(),
            },
        }
    }
}


pub trait LiveRaftNode {
    fn on_append_entries(&mut self, req: &rpc::AppendEntriesReq) -> rpc::AppendEntriesResp;
    fn on_request_vote(&mut self, req: &rpc::VoteReq) -> rpc::VoteResp;
    fn on_clock_tick(&mut self);
}


impl<T> Node<T> where Node<T>: LiveRaftNode {
    fn trigger(&self, event: Event) {
        self.noti_center.send(event).expect("send event");
    }

    fn apply(&self, entry: &rpc::Entry) {}

    fn apply_log(&mut self) {
        while self.commit_index > self.last_applied_id {
            self.last_applied_id += 1;
            let entry = self.log.get(self.last_applied_id as usize);
            self.apply(&entry.unwrap());
        }
    }

    fn last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    fn last_log_term(&self) -> u64 {
        let entry = self.log.get(self.last_log_index() as usize - 1);
        match entry {
            Some(e) => e.term,
            None => 0,
        }
    }

    fn prev_last_log_index(&self) -> u64 {
        if self.last_log_index() > 0 {
            self.last_log_index() - 1
        } else {
            self.last_log_index()
        }
    }

    fn prev_last_log_term(&self) -> u64 {
        let entry = self.log.get(self.prev_last_log_index() as usize - 1);
        match entry {
            Some(e) => e.term,
            None => 0,
        }
    }

    fn get_entry_at(&self, index: u64) -> Option<&rpc::Entry> {
        self.log.get(index as usize)
    }

    fn append_entry(&mut self, entry: rpc::Entry) {
        self.log.push(entry);
    }

    fn delete_entries_since(&mut self, index: u64) {
        self.log.truncate(index as usize);
    }
}


impl LiveRaftNode for Node<Follower> {
    fn on_request_vote(&mut self, req: &rpc::VoteReq) -> rpc::VoteResp {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }

        let mut vote_granted = false;
        if req.term == self.current_term {
            if self.voted_for.is_none() || self.voted_for == Some(req.candidate_id.clone()) {
                if req.last_log_index >= self.last_log_index() && req.last_log_term >= self.last_log_term() {
                    vote_granted = true;
                    self.voted_for = Some(req.candidate_id.clone());
                }
            }
        }

        rpc::VoteResp {
            term: self.current_term,
            vote_granted: vote_granted,
        }
    }

    fn on_append_entries(&mut self, req: &rpc::AppendEntriesReq) -> rpc::AppendEntriesResp {
        self.state.heartbeat_received_at = time::now_utc();

        if req.term < self.current_term {
            return rpc::AppendEntriesResp {
                term: self.current_term,
                success: false,
            };
        } else if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }

        {
            let prev_entry = self.get_entry_at(req.prev_log_index);
            if prev_entry.is_none() || prev_entry.unwrap().term != req.prev_log_term {
                return rpc::AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                };
            }
        }

        for (i, entry) in req.entries.iter().enumerate() {
            let index = req.prev_log_index + i as u64 + 1;
            let term = self.get_entry_at(index).and_then(|e| Some(e.term));
            match term {
                None => {
                    self.append_entry((*entry).clone());
                },
                Some(t) => {
                    if t != self.current_term {
                        self.delete_entries_since(index);
                    }
                },
            }
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = if req.leader_commit < self.last_log_index() {
                req.leader_commit
            } else {
                self.last_log_index()
            }
        }

        self.apply_log();

        rpc::AppendEntriesResp {
            term: self.current_term,
            success: true,
        }
    }

    fn on_clock_tick(&mut self) {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_received_at;
        if period_since_last_heartbeat.num_milliseconds() > ELECTION_TIMEOUT {
            self.trigger(Event::ConvertToCandidate);
        }
    }
}


impl Node<Candidate> {
    fn new_election(&mut self) {
        self.current_term += 1;
        self.state.election_started_at = time::now_utc();
        self.state.votes.clear();
        self.send_vote_request();
    }

    fn send_vote_request(&self) {
        self.servers
            .iter()
            .skip_while(|server|
                server.as_str() != self.server_id.as_str())
            .map(|server| {
                let req = rpc::VoteReq {
                    term: self.current_term,
                    candidate_id: self.server_id.clone(),
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                };
                self.trigger(Event::SendRequestVote((server.clone(), req)));
            });
    }

    fn on_receive_vote_request(&mut self, peer: &str, resp: rpc::VoteResp) {
        if resp.term > self.current_term {
            self.current_term = resp.term;
            self.trigger(Event::ConvertToFollower);
        }
        if resp.vote_granted {
            self.state.votes.insert(peer.to_string());
        }
    }
}


impl LiveRaftNode for Node<Candidate> {
    fn on_request_vote(&mut self, req: &rpc::VoteReq) -> rpc::VoteResp {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.trigger(Event::ConvertToFollower);
        }

        rpc::VoteResp {
            term: self.current_term,
            vote_granted: false,
        }
    }

    fn on_append_entries(&mut self, req: &rpc::AppendEntriesReq) -> rpc::AppendEntriesResp {
        if req.term > self.current_term {
            self.current_term = req.term;
        }

        self.trigger(Event::ConvertToFollower);
        rpc::AppendEntriesResp {
            term: self.current_term,
            success: false,
        }
    }

    fn on_clock_tick(&mut self) {
        {
            if self.state.votes.len() * 2 > self.servers.len() {
                self.trigger(Event::ConvertToLeader);
            }
        }

        let period_since_last_heartbeat = time::now_utc() - self.state.election_started_at;
        if period_since_last_heartbeat.num_milliseconds() > ELECTION_TIMEOUT {
            self.new_election()
        }
    }
}


impl LiveRaftNode for Node<Leader> {
    fn on_append_entries(&mut self, req: &rpc::AppendEntriesReq) -> rpc::AppendEntriesResp {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.trigger(Event::ConvertToFollower);
        }

        rpc::AppendEntriesResp {
            term: self.current_term,
            success: false,
        }
    }

    fn on_request_vote(&mut self, req: &rpc::VoteReq) -> rpc::VoteResp {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.trigger(Event::ConvertToFollower);
        }

        rpc::VoteResp {
            term: self.current_term,
            vote_granted: false,
        }
    }

    fn on_clock_tick(&mut self) {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_sent_at;
        if period_since_last_heartbeat.num_milliseconds() > HEARTBEAT_INTERVAL {
            self.send_heartbeat()
        }
    }
}


impl Node<Leader> {
    fn send_heartbeat(&self) {
        for s in &self.servers {
            self.send_append_entries_request(s);
        }
    }

    fn send_append_entries_request(&self, peer: &str) {
        let last_log_index = self.last_log_index();
        let next_index = self.state.next_index.get(peer).map_or(last_log_index + 1, |i| *i);
        let entries = if last_log_index >= next_index {
            self.log.iter().skip(next_index as usize - 1).cloned().collect::<Vec<rpc::Entry>>()
        } else {
            Vec::new()
        };

        let req = rpc::AppendEntriesReq {
            term: self.current_term,
            entries: entries,
            leader_commit: self.commit_index,
            leader_id: self.server_id.clone(),
            prev_log_index: self.prev_last_log_index(),
            prev_log_term: self.prev_last_log_term(),
        };

        self.trigger(Event::SendAppendEntries((peer.into(), req)));
    }

    fn send_append_entries_requests(&self) {
        self.servers.iter().skip_while(|s| s.as_str() != self.server_id.as_str())
            .map(|server| self.send_append_entries_request(server));
    }

    fn on_receive_append_entries_request(&mut self, peer: &str, resp: rpc::AppendEntriesResp) {
        self.state.heartbeat_sent_at = time::now_utc();
        let last_log_index = self.last_log_index();

        if resp.term > self.current_term {
            self.current_term = resp.term;
            self.trigger(Event::ConvertToFollower);
            return;
        }

        if !resp.success {
            let next_index = self.state.next_index.get(peer).map_or(last_log_index + 1, |i| *i);
            if next_index >= 1 {
                self.state.next_index.insert(peer.to_string(), next_index - 1);
                self.send_append_entries_request(peer);
                return;
            }
            unreachable!();
        }

        self.state.match_index.insert(peer.to_owned(), last_log_index);
        self.state.next_index.insert(peer.to_owned(), last_log_index + 1);

        let mut index = self.commit_index + 1;
        loop {
            let up_to_date_count = self.state.match_index.values().take_while(|&&match_index| match_index >= index).count();
            if up_to_date_count * 2 >= self.servers.len() {
                let term = self.get_entry_at(index as u64).and_then(|e| Some(e.term));
                if term == Some(self.current_term) {
                    self.commit_index = index;
                } else {
                    break;
                }
            }
            index += 1;
        }
    }

    fn on_receive_command(&mut self, command: String) -> String {
        let entry = rpc::Entry {
            term: self.current_term,
            payload: command,
        };
        self.append_entry(entry);

        self.send_append_entries_requests();

        self.apply_log();
        return "ok".into();
    }
}
