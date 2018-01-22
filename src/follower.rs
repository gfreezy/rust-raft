use time;
use std::collections::HashSet;
use std::sync::mpsc::Sender;
use std::fmt;

use rpc::{AppendEntriesReq, AppendEntriesResp, EntryIndex, EntryPayload, ServerId, Term, VoteReq, VoteResp};
use entry_log::EntryLog;
use store::Store;
use request::Request;

use node::{Node, NodeListener, ELECTION_TIMEOUT};
use candidate::Candidate;
use leader::Leader;

#[derive(Debug)]
pub struct Follower {
    heartbeat_received_at: time::Tm,
}

impl<S: Store> Node<Follower, S> {
    pub fn new(
        server_id: ServerId,
        store: S,
        servers: Vec<ServerId>,
        noti_center: Sender<Request>,
    ) -> Self {
        Node {
            server_id,
            current_term: Term(0),
            voted_for: None,
            log: EntryLog::new(),
            commit_index: EntryIndex(::NEXT_DATA_INDEX - 1),
            last_applied_id: EntryIndex(::NEXT_DATA_INDEX - 1),
            state: Follower {
                heartbeat_received_at: time::now_utc(),
            },
            noti_center,
            servers: servers.into_iter().collect::<HashSet<ServerId>>(),
            new_servers: HashSet::new(),
            non_voting_servers: HashSet::new(),
            store,
        }
    }

    fn is_heartbeat_expired(&self) -> bool {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_received_at;
        if period_since_last_heartbeat.num_milliseconds() > ELECTION_TIMEOUT as i64 {
            return true;
        }
        false
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
            new_servers: val.new_servers,
            non_voting_servers: val.non_voting_servers,
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
            new_servers: val.new_servers,
            non_voting_servers: val.non_voting_servers,
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
    fn on_request_vote(&mut self, req: VoteReq, remote_addr: ServerId) -> (VoteResp, bool) {
        // disregard RequestVote RPCs when we believe a current leader exists.
        if !self.is_heartbeat_expired() {
            return (
                VoteResp {
                    term: self.current_term,
                    vote_granted: false,
                },
                false,
            );
        }

        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }

        let mut vote_granted = false;
        if (req.term == self.current_term)
            && (self.voted_for.is_none() || self.voted_for == Some(req.candidate_id.clone()))
            && (req.last_log_index >= self.log.last_index()
                && req.last_log_term >= self.log.last_entry_term())
        {
            vote_granted = true;
            self.voted_for = Some(req.candidate_id.clone());
        }

        info!(
            "{} term: {}, vote granted: {}",
            self, self.current_term, vote_granted
        );
        (
            VoteResp {
                term: self.current_term,
                vote_granted: vote_granted,
            },
            false,
        )
    }

    fn on_append_entries(
        &mut self,
        req: AppendEntriesReq,
        remote_addr: ServerId,
    ) -> (AppendEntriesResp, bool) {
        self.state.heartbeat_received_at = time::now_utc();

        if req.term < self.current_term {
            return (
                AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                },
                false,
            );
        } else if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }

        if req.prev_log_index > EntryIndex(0) {
            let prev_entry = self.log.get(req.prev_log_index);
            if prev_entry.is_none() || prev_entry.unwrap().term != req.prev_log_term {
                return (
                    AppendEntriesResp {
                        term: self.current_term,
                        success: false,
                    },
                    false,
                );
            }
        }

        for (i, entry) in req.entries.iter().enumerate() {
            let index = req.prev_log_index + i + 1;
            let term = self.log.get(index).and_then(|e| Some(e.term));
            match term {
                None => {
                    self.log.push((*entry).clone());
                    match entry.payload {
                        EntryPayload::Config(ref servers) => {
                            use std::iter::FromIterator;
                            self.servers = HashSet::from_iter(servers.iter().cloned());
                        }
                        EntryPayload::Data(_) => {}
                    }
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

        if !req.entries.is_empty() {
            info!("{} term: {}, on append entries", self, self.current_term);
        }

        (
            AppendEntriesResp {
                term: self.current_term,
                success: true,
            },
            false,
        )
    }

    fn on_clock_tick(&mut self) -> bool {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_received_at;
        if period_since_last_heartbeat.num_milliseconds() > ELECTION_TIMEOUT as i64 {
            return true;
        }
        false
    }
}
