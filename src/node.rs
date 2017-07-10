use time;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;
use rand;
use std::fmt;

use ::rpc::{ServerId, Term, EntryIndex, Entry, VoteReq, VoteResp, AppendEntriesReq, AppendEntriesResp, CommandReq, CommandResp};
use ::entry_log::EntryLog;
use ::store::Store;
use ::request::Request;


// 500ms timeout
const ELECTION_TIMEOUT: u64 = 1000;
const HEARTBEAT_INTERVAL: u64 = 100;


pub trait NodeListener {
    fn on_append_entries(&mut self, req: &AppendEntriesReq) -> (AppendEntriesResp, bool);
    fn on_request_vote(&mut self, req: &VoteReq) -> (VoteResp, bool);
    fn on_clock_tick(&mut self) -> bool;
}


#[derive(Debug)]
pub struct Node<T, S: Store> {
    server_id: ServerId,
    current_term: Term,
    voted_for: Option<ServerId>,
    log: EntryLog,
    commit_index: EntryIndex,
    last_applied_id: EntryIndex,
    noti_center: Sender<Request>,
    servers: HashSet<ServerId>,
    state: T,
    store: S,
}

impl<T, S: Store> Node<T, S> {
    fn trigger(&self, req: Request) {
        self.noti_center.send(req).expect("send event");
    }

    fn apply_log(&mut self) {
        while self.commit_index > self.last_applied_id {
            self.last_applied_id.incr();
            let entry = self.log.get(self.last_applied_id);
            if let Some(e) = entry {
                self.store.apply(e)
            }
        }
    }

    fn peers(&self) -> Vec<ServerId> {
        self.servers
            .iter()
            .filter(|server| {
                **server != self.server_id
            }).cloned().collect::<Vec<ServerId>>()
    }
}


#[derive(Debug)]
pub struct Follower {
    heartbeat_received_at: time::Tm,
}

impl<S: Store> Node<Follower, S> {
    pub fn new(server_id: ServerId, store: S, servers: Vec<ServerId>, noti_center: Sender<Request>) -> Self {
        Node {
            server_id: server_id,
            current_term: Term(0),
            voted_for: None,
            log: EntryLog::new(),
            commit_index: EntryIndex(0),
            last_applied_id: EntryIndex(0),
            state: Follower {
                heartbeat_received_at: time::now_utc(),
            },
            noti_center: noti_center,
            servers: servers.into_iter().collect::<HashSet<ServerId>>(),
            store: store,
        }
    }
}


impl<S: Store> fmt::Display for Node<Follower, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Node<Follower, {}>", &self.server_id)
    }
}


impl<S: Store> From<Node<Candidate, S>> for Node<Follower, S> {
    fn from(val: Node<Candidate, S>) -> Self {
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
            store: val.store,
        };
        info!("from Candidate to Follower");
        n
    }
}


impl<S: Store> From<Node<Leader, S>> for Node<Follower, S> {
    fn from(val: Node<Leader, S>) -> Self {
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
            store: val.store,
        };
        info!("from Leader to Follower");
        n
    }
}

impl<S: Store> NodeListener for Node<Follower, S> {
    fn on_request_vote(&mut self, req: &VoteReq) -> (VoteResp, bool) {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }

        let mut vote_granted = false;
        if (req.term == self.current_term)
            && (self.voted_for.is_none() || self.voted_for == Some(req.candidate_id.clone()))
            && (req.last_log_index >= self.log.last_index() && req.last_log_term >= self.log.last_entry_term()) {
            vote_granted = true;
            self.voted_for = Some(req.candidate_id.clone());
        }


        info!("{} term: {}, vote granted: {}", self, self.current_term, vote_granted);
        (VoteResp {
            term: self.current_term,
            vote_granted: vote_granted,
        }, false)
    }

    fn on_append_entries(&mut self, req: &AppendEntriesReq) -> (AppendEntriesResp, bool) {
        self.state.heartbeat_received_at = time::now_utc();

        if req.term < self.current_term {
            return (AppendEntriesResp {
                term: self.current_term,
                success: false,
            }, false);
        } else if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }

        if req.prev_log_index > EntryIndex(0) {
            let prev_entry = self.log.get(req.prev_log_index);
            if prev_entry.is_none() || prev_entry.unwrap().term != req.prev_log_term {
                return (AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                }, false);
            }
        }

        for (i, entry) in req.entries.iter().enumerate() {
            let index = req.prev_log_index + i + 1;
            let term = self.log.get(index).and_then(|e| Some(e.term));
            match term {
                None => {
                    self.log.push((*entry).clone());
                }
                Some(t) => {
                    if t != self.current_term {
                        self.log.truncate(index);
                    }
                }
            }
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = if req.leader_commit < self.log.last_index() {
                req.leader_commit
            } else {
                self.log.last_index()
            }
        }

        self.apply_log();

        info!("{} term: {}, on append entries", self, self.current_term);

        (AppendEntriesResp {
            term: self.current_term,
            success: true,
        }, false)
    }

    fn on_clock_tick(&mut self) -> bool {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_received_at;
        if period_since_last_heartbeat.num_milliseconds() > ELECTION_TIMEOUT as i64 {
            return true;
        }
        false
    }
}


#[derive(Debug)]
pub struct Candidate {
    votes: HashSet<ServerId>,
    election_started_at: time::Tm,
    election_timeout: u64,
}


impl<S: Store> Node<Candidate, S> {
    fn new_election(&mut self) {
        self.current_term.incr();
        self.state.election_started_at = time::now_utc();

        let sample: f64 = rand::random();
        let election_timeout = ((sample + 1.0) * ELECTION_TIMEOUT as f64) as u64;
        self.state.election_timeout = election_timeout;

        self.state.votes.clear();
        self.state.votes.insert(self.server_id.clone());
        info!("{} new election, timeout: {}", self.current_term, election_timeout);
        self.send_vote_request();
    }

    fn send_vote_request(&self) {
        let votes = self.peers().into_iter().map(|server| {
            (server, VoteReq {
                term: self.current_term,
                candidate_id: self.server_id.clone(),
                last_log_index: self.log.last_index(),
                last_log_term: self.log.last_entry_term()
            })
        }).collect::<Vec<(ServerId, VoteReq)>>();
        self.trigger(Request::VoteFor(votes));
    }

    pub fn on_receive_vote_request(&mut self, peer: &ServerId, _req: VoteReq, resp: VoteResp) -> bool {
        if resp.term > self.current_term {
            self.current_term = resp.term;
            return true;
        }

        if resp.vote_granted {
            self.state.votes.insert(peer.clone());
        }
        false
    }
}

impl<S: Store> fmt::Display for Node<Candidate, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Node<Candidate, {}>", &self.server_id)
    }
}


impl<S: Store> From<Node<Follower, S>> for Node<Candidate, S> {
    fn from(val: Node<Follower, S>) -> Self {
        let mut n: Node<Candidate, S> = Node {
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
            store: val.store,
        };
        n.new_election();
        info!("from Follower to Candidate");
        n
    }
}


impl<S: Store> NodeListener for Node<Candidate, S> {
    fn on_request_vote(&mut self, req: &VoteReq) -> (VoteResp, bool) {
        let mut to_follower = false;
        if req.term > self.current_term {
            self.current_term = req.term;
            to_follower = true;
        }

        (VoteResp {
            term: self.current_term,
            vote_granted: false,
        }, to_follower)
    }

    fn on_append_entries(&mut self, req: &AppendEntriesReq) -> (AppendEntriesResp, bool) {
        if req.term > self.current_term {
            self.current_term = req.term;
        }

        (AppendEntriesResp {
            term: self.current_term,
            success: false,
        }, true)
    }

    fn on_clock_tick(&mut self) -> bool {
        {
            if self.state.votes.len() * 2 > self.servers.len() {
                return true;
            }
        }

        let period_since_last_heartbeat = time::now_utc() - self.state.election_started_at;
        if period_since_last_heartbeat.num_milliseconds() > self.state.election_timeout as i64 {
            self.new_election()
        }
        false
    }
}


#[derive(Debug)]
pub struct Leader {
    next_index: HashMap<ServerId, EntryIndex>,
    match_index: HashMap<ServerId, EntryIndex>,
    heartbeat_sent_at: time::Tm,
    heartbeats: HashSet<ServerId>,
}


impl<S: Store> Node<Leader, S> {
    fn send_heartbeat(&mut self) {
        self.state.heartbeats.clear();
        self.send_append_entries_requests(self.peers());
    }

    fn create_append_entries_request(&self, peer: ServerId) -> (ServerId, AppendEntriesReq) {
        let last_log_index = self.log.last_index();
        let next_index = self.state.next_index.get(&peer).map_or(last_log_index + 1, |i| *i);
        let entries = if last_log_index >= next_index {
            self.log.get_entries_since_index(next_index)
        } else {
            Vec::new()
        };

        let req = AppendEntriesReq {
            term: self.current_term,
            entries: entries,
            leader_commit: self.commit_index,
            leader_id: self.server_id.clone(),
            prev_log_index: self.log.prev_last_index(),
            prev_log_term: self.log.prev_last_entry_term(),
        };

        (peer, req)
    }

    fn send_append_entries_requests(&self, peers: Vec<ServerId>) {
        self.trigger(
            Request::AppendEntriesFor(
                peers.into_iter().map(|server| {
                    self.create_append_entries_request(server)
                }).collect()
            )
        );
    }

    pub fn on_receive_append_entries_request(&mut self, peer: &ServerId, req: AppendEntriesReq, resp: AppendEntriesResp) -> bool {
        let last_log_index = self.log.last_index();

        if resp.term > self.current_term {
            self.current_term = resp.term;
            info!("on_receive_append_entries_request: Leader Convert to follower");
            return true;
        }

        // move next_index backwards by one
        if !resp.success {
            info!("{}: on_receive_append_entries_request: not success", peer);
            let next_index = self.state.next_index.get(peer).map_or_else(|| last_log_index + 1, |i| *i);
            info!("{}: on_receive_append_entries_request: next index {}", peer, next_index);

            self.state.next_index.insert(peer.clone(), next_index.prev_or_zero());
            self.send_append_entries_requests(vec![peer.clone()]);
            return false;
        }

        if req.entries.len() == 0 {
            self.state.heartbeats.insert(peer.clone());
            let up_to_date_count = self.state.heartbeats.len();
            debug!("update to date count: {}", up_to_date_count);
            if up_to_date_count * 2 >= self.servers.len() {
                self.state.heartbeat_sent_at = time::now_utc();
                debug!("heartbeat {:?} {:?}", time::now(), self.current_term);
            }
        } else {
            info!("on_receive_append_entries_request: before insert. last_log_index: {}", last_log_index);
            self.state.match_index.insert(peer.to_owned(), last_log_index);
            self.state.next_index.insert(peer.to_owned(), last_log_index + 1);

            let mut index = self.commit_index + 1;
            loop {
                info!("in loop: index {}, ", index);
                let up_to_date_count = self.state.match_index.values().filter(|&&match_index| match_index >= index).count();
                info!("update to date count: {}", up_to_date_count);
                if up_to_date_count * 2 < self.servers.len() {
                    break;
                }

                let term = self.log.get(index).and_then(|e| Some(e.term));
                info!("term: {:?}, current_term: {:?}", term, self.current_term);
                if term == Some(self.current_term) {
                    self.commit_index = index;
                    self.state.heartbeat_sent_at = time::now_utc();
                } else {
                    break;
                }

                index.incr();
            }
        }
        false
    }

    pub fn on_receive_command(&mut self, command: CommandReq) -> CommandResp {
        let entry = Entry {
            term: self.current_term,
            payload: command.0,
        };
        self.log.push(entry);

        self.send_append_entries_requests(self.peers());

        self.apply_log();
        CommandResp("ok".into())
    }
}

impl<S: Store> fmt::Display for Node<Leader, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Node<Leader, {}>", &self.server_id)
    }
}

impl<S: Store> NodeListener for Node<Leader, S> {
    fn on_append_entries(&mut self, req: &AppendEntriesReq) -> (AppendEntriesResp, bool) {
        let mut to_follower = false;
        if req.term > self.current_term {
            self.current_term = req.term;
            to_follower = true;
        }

        (AppendEntriesResp {
            term: self.current_term,
            success: false,
        }, to_follower)
    }

    fn on_request_vote(&mut self, req: &VoteReq) -> (VoteResp, bool) {
        let mut to_follower = false;
        if req.term > self.current_term {
            self.current_term = req.term;
            to_follower = true;
        }

        (VoteResp {
            term: self.current_term,
            vote_granted: false,
        }, to_follower)
    }

    fn on_clock_tick(&mut self) -> bool {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_sent_at;
        if period_since_last_heartbeat.num_milliseconds() > HEARTBEAT_INTERVAL as i64 {
            debug!("leader: send heartbeat");
            self.send_heartbeat()
        }
        false
    }
}


impl<S: Store> From<Node<Candidate, S>> for Node<Leader, S> {
    fn from(val: Node<Candidate, S>) -> Self {
        let last_log_index = val.log.last_index();
        let next_index = {
            let mut map = HashMap::new();
            for s in &val.servers {
                map.insert(s.clone(), last_log_index);
            }
            map
        };

        let match_index = {
            let mut map = HashMap::new();
            for s in &val.servers {
                map.insert(s.clone(), EntryIndex(0));
            }
            map
        };

        let mut node = Node {
            server_id: val.server_id,
            current_term: val.current_term,
            voted_for: val.voted_for,
            log: val.log,
            commit_index: val.commit_index,
            last_applied_id: val.last_applied_id,
            noti_center: val.noti_center,
            state: Leader {
                next_index: next_index,
                match_index: match_index,
                heartbeat_sent_at: time::now_utc(),
                heartbeats: HashSet::new(),
            },
            servers: val.servers,
            store: val.store,
        };

        node.send_heartbeat();
        info!("from Candidate to Leader");
        node
    }
}
