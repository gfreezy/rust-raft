use time;
use std::collections::HashSet;
use rand;
use std::fmt;

use rpc::{AppendEntriesReq, AppendEntriesResp, ServerId, VoteReq, VoteResp};
use store::Store;
use request::Request;

use node::{Node, NodeListener, ELECTION_TIMEOUT};
use follower::Follower;


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
        info!(
            "{} new election, timeout: {}",
            self.current_term, election_timeout
        );
        self.send_vote_request();
    }

    fn send_vote_request(&self) {
        let votes = self.peers()
            .into_iter()
            .map(|server| {
                (
                    server,
                    VoteReq {
                        term: self.current_term,
                        candidate_id: self.server_id.clone(),
                        last_log_index: self.log.last_index(),
                        last_log_term: self.log.last_entry_term(),
                    },
                )
            })
            .collect::<Vec<(ServerId, VoteReq)>>();
        self.trigger(Request::VoteFor(votes));
    }

    pub fn on_receive_vote_request(
        &mut self,
        peer: &ServerId,
        _req: VoteReq,
        resp: VoteResp,
    ) -> bool {
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
            new_servers: val.new_servers,
            non_voting_servers: val.non_voting_servers,
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
    fn on_request_vote(&mut self, req: VoteReq, remote_addr: ServerId) -> (VoteResp, bool) {
        let to_follower = if req.term > self.current_term {
            self.current_term = req.term;
            true
        } else {
            false
        };

        (
            VoteResp {
                term: self.current_term,
                vote_granted: false,
            },
            to_follower,
        )
    }

    fn on_append_entries(
        &mut self,
        req: AppendEntriesReq,
        remote_addr: ServerId,
    ) -> (AppendEntriesResp, bool) {
        if req.term > self.current_term {
            self.current_term = req.term;
        }

        (
            AppendEntriesResp {
                term: self.current_term,
                success: false,
            },
            true,
        )
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
