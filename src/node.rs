use time;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;
use rand;
use std::fmt;

use ::rpc;
use ::event::Event;
use ::raft_node::LiveRaftNode;


// 500ms timeout
const ELECTION_TIMEOUT: u64 = 1000;
const HEARTBEAT_INTERVAL: u64 = 100;


#[derive(Debug)]
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

impl<T> Node<T> where Node<T>: LiveRaftNode {
    fn trigger(&self, event: Event) {
        self.noti_center.send(event).expect("send event");
    }

    #[allow(unused_variables)]
    fn apply(&self, entry: &rpc::Entry) {}

    fn apply_log(&mut self) {
        while self.commit_index > self.last_applied_id {
            self.last_applied_id += 1;
            let entry = self.log.get(self.last_applied_id as usize);
            self.apply(entry.unwrap());
        }
    }

    fn last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    fn last_log_term(&self) -> u64 {
        let last_log_index = self.last_log_index() as usize;
        let entry = self.log.get(if last_log_index > 0 { last_log_index - 1 } else { 0 });
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
        let last_log_index = self.prev_last_log_index() as usize;
        let entry = self.log.get(if last_log_index > 0 { last_log_index - 1 } else { 0 });
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

    fn peers(&self) -> Vec<String> {
        self.servers
            .iter()
            .filter(|server| {
                server.as_str() != self.server_id.as_str()
            }).cloned().collect::<Vec<String>>()
    }
}


#[derive(Debug)]
pub struct Follower {
    heartbeat_received_at: time::Tm,
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


impl fmt::Display for Node<Follower> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Node<Follower, {}>", &self.server_id)
    }
}


impl From<Node<Candidate>> for Node<Follower> {
    fn from(val: Node<Candidate>) -> Self {
        let n = Node {
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
        };
        info!("from Candidate to Follower");
        n
    }
}


impl From<Node<Leader>> for Node<Follower> {
    fn from(val: Node<Leader>) -> Self {
        let n = Node {
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
        };
        info!("from Leader to Follower");
        n
    }
}

impl LiveRaftNode for Node<Follower> {
    fn on_request_vote(&mut self, req: &rpc::VoteReq) -> rpc::VoteResp {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }

        let mut vote_granted = false;
        if (req.term == self.current_term)
            && (self.voted_for.is_none() || self.voted_for == Some(req.candidate_id.clone()))
            && (req.last_log_index >= self.last_log_index() && req.last_log_term >= self.last_log_term()) {
            vote_granted = true;
            self.voted_for = Some(req.candidate_id.clone());
        }


        info!("{} term: {}, vote granted: {}", self, self.current_term, vote_granted);
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

        info!("{} term: {}, on append entries", self, self.current_term);

        rpc::AppendEntriesResp {
            term: self.current_term,
            success: true,
        }
    }

    fn on_clock_tick(&mut self) {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_received_at;
        if period_since_last_heartbeat.num_milliseconds() > ELECTION_TIMEOUT as i64 {
            self.trigger(Event::ConvertToCandidate);
        }
    }
}


#[derive(Debug)]
pub struct Candidate {
    votes: HashSet<String>,
    election_started_at: time::Tm,
    election_timeout: u64,
}


impl Node<Candidate> {
    fn new_election(&mut self) {
        self.current_term += 1;
        self.state.election_started_at = time::now_utc();

        let sample: f64 = rand::random();
        let election_timeout = ((sample + 1.0) * ELECTION_TIMEOUT as f64) as u64;
        self.state.election_timeout = election_timeout;

        self.state.votes.clear();
        self.state.votes.insert(self.server_id.to_string());
        info!("{} new election, timeout: {}", self.current_term, election_timeout);
        self.send_vote_request();
    }

    fn send_vote_request(&self) {
        for server in &self.peers() {
            let req = rpc::VoteReq {
                term: self.current_term,
                candidate_id: self.server_id.clone(),
                last_log_index: self.last_log_index(),
                last_log_term: self.last_log_term(),
            };
            self.trigger(Event::SendRequestVote((server.clone(), req)));
        }
    }

    pub fn on_receive_vote_request(&mut self, peer: &str, resp: rpc::VoteResp) {
        if resp.term > self.current_term {
            self.current_term = resp.term;
            info!("before trigger ConvertToFollower");
            self.trigger(Event::ConvertToFollower);
            info!("after trigger ConvertToFollower");
        }

        if resp.vote_granted {
            self.state.votes.insert(peer.to_string());
        }
    }
}

impl fmt::Display for Node<Candidate> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Node<Candidate, {}>", &self.server_id)
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
                election_timeout: ELECTION_TIMEOUT,
            },
        };
        n.new_election();
        info!("from Follower to Candidate");
        n
    }
}


impl LiveRaftNode for Node<Candidate> {
    fn on_request_vote(&mut self, req: &rpc::VoteReq) -> rpc::VoteResp {
        if req.term > self.current_term {
            self.current_term = req.term;
            info!("before trigger");
            self.trigger(Event::ConvertToFollower);
            info!("after trigger");
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
        if period_since_last_heartbeat.num_milliseconds() > self.state.election_timeout as i64 {
            self.new_election()
        }
    }
}


#[derive(Debug)]
pub struct Leader {
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
    heartbeat_sent_at: time::Tm,
}


impl Node<Leader> {
    fn send_heartbeat(&self) {
        for s in &self.peers() {
            self.send_append_entries_request(s);
        }
    }

    fn send_append_entries_request(&self, peer: &str) {
        let last_log_index = self.last_log_index();
        let next_index = self.state.next_index.get(peer).map_or(last_log_index + 1, |i| *i);
        info!("a");
        let entries = if last_log_index >= next_index && next_index > 1 {
            self.log.iter().skip(next_index as usize - 1).cloned().collect::<Vec<rpc::Entry>>()
        } else {
            Vec::new()
        };
        info!("b");

        let req = rpc::AppendEntriesReq {
            term: self.current_term,
            entries: entries,
            leader_commit: self.commit_index,
            leader_id: self.server_id.clone(),
            prev_log_index: self.prev_last_log_index(),
            prev_log_term: self.prev_last_log_term(),
        };
        info!("c");

        self.trigger(Event::SendAppendEntries((peer.into(), req)));
        info!("d");
    }

    #[allow(dead_code)]
    fn send_append_entries_requests(&self) {
        for server in &self.peers() {
            self.send_append_entries_request(server);
        }
    }

    pub fn on_receive_append_entries_request(&mut self, peer: &str, resp: rpc::AppendEntriesResp) {
        self.state.heartbeat_sent_at = time::now_utc();
        let last_log_index = self.last_log_index();

        if resp.term > self.current_term {
            self.current_term = resp.term;
            self.trigger(Event::ConvertToFollower);
            info!("on_receive_append_entries_request: Leader Convert to follower");
            return;
        }

        if !resp.success {
            info!("on_receive_append_entries_request: not success");
            let next_index = self.state.next_index.get(peer).map_or(last_log_index + 1, |i| *i);
            info!("on_receive_append_entries_request: nex index");

            if next_index >= 1 {
                self.state.next_index.insert(peer.to_string(), next_index - 1);
                self.send_append_entries_request(peer);
                return;
            }
        }

        info!("on_receive_append_entries_request: before insert");
        self.state.match_index.insert(peer.to_owned(), last_log_index);
        self.state.next_index.insert(peer.to_owned(), last_log_index + 1);

        let mut index = self.commit_index + 1;
        loop {
            info!("in loop: index {}, ", index);
            let up_to_date_count = self.state.match_index.values().take_while(|&&match_index| match_index >= index).count();
            info!("update to date count: {}", up_to_date_count);
            if up_to_date_count * 2 < self.servers.len() {
                break;
            }

            let term = self.get_entry_at(index as u64).and_then(|e| Some(e.term));
            info!("term: {:?}, current_term: {:?}", term, self.current_term);
            if term == Some(self.current_term) {
                self.commit_index = index;
            } else {
                break;
            }

            index += 1;
        }
    }

    #[allow(dead_code)]
    pub fn on_receive_command(&mut self, command: String) -> String {
        let entry = rpc::Entry {
            term: self.current_term,
            payload: command,
        };
        self.append_entry(entry);

        self.send_append_entries_requests();

        self.apply_log();
        "ok".into()
    }
}

impl fmt::Display for Node<Leader> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Node<Leader, {}>", &self.server_id)
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
            info!("before trigger");
            self.trigger(Event::ConvertToFollower);
            info!("after trigger");
        }

        rpc::VoteResp {
            term: self.current_term,
            vote_granted: false,
        }
    }

    fn on_clock_tick(&mut self) {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_sent_at;
        if period_since_last_heartbeat.num_milliseconds() > HEARTBEAT_INTERVAL as i64 {
            self.send_heartbeat()
        }
    }
}


impl From<Node<Candidate>> for Node<Leader> {
    fn from(val: Node<Candidate>) -> Self {
        let node = Node {
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
        info!("from Candidate to Leader");
        node
    }
}
