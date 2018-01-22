use time;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::mem;

use ::rpc::{ServerId, EntryIndex, Entry, VoteReq, VoteResp, AppendEntriesReq, AppendEntriesResp, CommandReq, CommandResp,
            ConfigurationReq, EntryPayload, EntryDataPayload};
use ::store::Store;
use ::request::Request;

use ::node::{Node, NodeListener, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL};
use ::candidate::Candidate;


#[derive(Debug)]
pub struct Leader {
    next_index: HashMap<ServerId, EntryIndex>,
    match_index: HashMap<ServerId, EntryIndex>,
    heartbeat_sent_at: time::Tm,
    heartbeats: HashSet<ServerId>,
    planed_new_servers: HashSet<ServerId>,
}


impl<S: Store> Node<Leader, S> {
    fn is_leader_alive(&self) -> bool {
        let period_since_last_heartbeat = time::now_utc() - self.state.heartbeat_sent_at;
        if period_since_last_heartbeat.num_milliseconds() <= ELECTION_TIMEOUT as i64 {
            return true;
        }
        false
    }

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
            prev_log_index: self.log.prev_index(next_index),
            prev_log_term: self.log.prev_entry_term(next_index),
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

        // heartbeat
        if req.entries.is_empty() {
            self.state.heartbeats.insert(peer.clone());
            let up_to_date_count = self.state.heartbeats.len();
            debug!("update to date count: {}", up_to_date_count);
            if up_to_date_count * 2 >= self.servers.len() {
                self.state.heartbeat_sent_at = time::now_utc();
                debug!("heartbeat {:?} {:?}", time::now(), self.current_term);
            }
            return false;
        }

        // append entries
        info!("on_receive_append_entries_request: before insert. last_log_index: {}", last_log_index);
        let old_next_index = self.state.next_index.get(peer).map_or(last_log_index, |index| *index);
        let new_next_index = old_next_index + req.entries.len();
        self.state.match_index.insert(peer.to_owned(), new_next_index);
        self.state.next_index.insert(peer.to_owned(), new_next_index + 1);

        // TODO: 需要修改
        if self.state.planed_new_servers.contains(peer) {
            return false;
        }

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
                self.apply_log();

                if !self.new_servers.is_empty() {
                    self.transition_to_new_config();
                }
            } else {
                break;
            }

            index.incr();
        }
        false
    }

    pub fn on_receive_command(&mut self, command: CommandReq) -> CommandResp {
        let entry = Entry {
            term: self.current_term,
            payload: EntryPayload::Data(EntryDataPayload {
                cmd: command.cmd,
                payload: command.data,
            })
        };
        self.log.push(entry);
        self.send_append_entries_requests(self.peers());

        CommandResp("ok".into())
    }

    pub fn on_update_configuration(&mut self, req: ConfigurationReq, remote_server: ServerId) {
        use std::iter::FromIterator;
        self.state.planed_new_servers = HashSet::from_iter(req.servers);

        let has_new_servers = self.state.planed_new_servers.difference(&self.servers).count() > 0;
        if !has_new_servers {
            self.transition_to_old_new_mixed_config();
        } else {
            self.send_append_entries_requests(self.state.planed_new_servers.iter().cloned().collect());
        }
    }

    fn transition_to_old_new_mixed_config(&mut self) {
        use std::iter::FromIterator;
        mem::swap(&mut self.new_servers, &mut self.state.planed_new_servers);
        let merged_servers: Vec<ServerId> = self.servers.union(&self.new_servers).cloned().collect();
        let entry = Entry {
            term: self.current_term,
            payload: EntryPayload::Config(merged_servers.clone())
        };
        self.log.push(entry);
        self.servers = HashSet::from_iter(merged_servers);
        self.send_append_entries_requests(self.peers());
    }

    fn transition_to_new_config(&mut self) {
        // replicate new config to the cluster
        if !self.new_servers.is_empty() {
            let mut new_servers = HashSet::new();
            mem::swap(&mut new_servers, &mut self.new_servers);
            use std::iter::FromIterator;
            let entry = Entry {
                term: self.current_term,
                payload: EntryPayload::Config(Vec::from_iter(new_servers.iter().cloned()))
            };
            self.log.push(entry);
            self.servers = new_servers;
            self.send_append_entries_requests(self.peers());
        }
    }
}

impl<S: Store> fmt::Display for Node<Leader, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Node<Leader, {}>", &self.server_id)
    }
}

impl<S: Store> NodeListener for Node<Leader, S> {
    fn on_append_entries(&mut self, req: AppendEntriesReq, remote_addr: ServerId) -> (AppendEntriesResp, bool) {
        let to_follower = if req.term > self.current_term {
            self.current_term = req.term;
            true
        } else {
            false
        };

        (AppendEntriesResp {
            term: self.current_term,
            success: false,
        }, to_follower)
    }

    fn on_request_vote(&mut self, req: VoteReq, remote_addr: ServerId) -> (VoteResp, bool) {
        if self.is_leader_alive() {
            return (VoteResp {
                term: self.current_term,
                vote_granted: false,
            }, false);
        }

        let to_follower = if req.term > self.current_term {
            self.current_term = req.term;
            true
        } else {
            false
        };

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
                map.insert(s.clone(), EntryIndex(::ZERO_INDEX));
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
                planed_new_servers: HashSet::new(),
            },
            servers: val.servers,
            new_servers: val.new_servers,
            non_voting_servers: val.non_voting_servers,
            store: val.store,
        };

        node.send_heartbeat();
        info!("from Candidate to Leader");
        println!("I'm Leader");
        node
    }
}
