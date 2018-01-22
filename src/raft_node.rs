use std::sync::mpsc::Sender;
use std::fmt;

use ::node::Node;
use ::leader::Leader;
use ::follower::Follower;
use ::candidate::Candidate;
use ::rpc::{ServerId, AppendEntriesReq, AppendEntriesResp, VoteReq, VoteResp, CommandResp, CommandReq, ConfigurationReq, ConfigurationResp};
use ::store::Store;
use ::node::NodeListener;
use ::request;


#[derive(Debug)]
enum RaftNode<S: Store> {
    Leader(Node<Leader, S>),
    Follower(Node<Follower, S>),
    Candidate(Node<Candidate, S>),
}


impl<S: Store> fmt::Display for RaftNode<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let x = match *self {
            RaftNode::Leader(ref x) => format!("{}", x),
            RaftNode::Follower(ref x) => format!("{}", x),
            RaftNode::Candidate(ref x) => format!("{}", x),
        };
        write!(f, "{}", x)
    }
}


#[derive(Debug)]
pub struct RaftStore<S: Store>(Option<RaftNode<S>>);


impl<S: Store> RaftStore<S> {
    pub fn new(server_id: ServerId, store: S, servers: Vec<ServerId>, noti_center: Sender<request::Request>) -> Self {
        RaftStore(Some(RaftNode::Follower(Node::new(server_id, store, servers, noti_center))))
    }

    pub fn on_receive_vote_request(&mut self, peer: &ServerId, req: VoteReq, resp: VoteResp) {
        use self::RaftNode::*;

        let mut raft_node = self.0.take().unwrap();

        let to_follower = match raft_node {
            Candidate(ref mut node) => {
                node.on_receive_vote_request(peer, req, resp)
            }
            _ => {
                false
            }
        };

        self.0 = Some(
            if to_follower {
                match raft_node {
                    Leader(node) => RaftNode::Follower(node.into()),
                    Follower(node) => RaftNode::Follower(node),
                    Candidate(node) => RaftNode::Follower(node.into())
                }
            } else {
                raft_node
            }
        );
    }

    pub fn on_receive_append_entries_request(&mut self, peer: &ServerId, req: AppendEntriesReq, resp: AppendEntriesResp) {
        use self::RaftNode::*;

        let mut raft_node = self.0.take().unwrap();

        let to_follower = match raft_node {
            Leader(ref mut node) => {
                node.on_receive_append_entries_request(peer, req, resp)
            }
            _ => {
                false
            }
        };

        self.0 = Some(
            if to_follower {
                match raft_node {
                    Leader(node) => RaftNode::Follower(node.into()),
                    Follower(node) => RaftNode::Follower(node),
                    Candidate(node) => RaftNode::Follower(node.into())
                }
            } else {
                raft_node
            }
        );
    }

    pub fn on_receive_command(&mut self, command: CommandReq) -> CommandResp {
        match self.0 {
            Some(RaftNode::Leader(ref mut node)) => node.on_receive_command(command),
            _ => CommandResp("error".to_string())
        }
    }
}


pub trait LiveRaftStore {
    fn on_append_entries(&mut self, req: AppendEntriesReq, remote_addr: ServerId) -> AppendEntriesResp;
    fn on_request_vote(&mut self, req: VoteReq, remote_addr: ServerId) -> VoteResp;
    fn on_update_configuration(&mut self, req: ConfigurationReq, remote_addr: ServerId) -> ConfigurationResp;
    fn on_clock_tick(&mut self);
}


impl<S: Store> LiveRaftStore for RaftStore<S> {
    fn on_request_vote(&mut self, req: VoteReq, remote_addr: ServerId) -> VoteResp {
        use self::RaftNode::*;

        let mut raft_node = self.0.take().unwrap();

        let (resp, to_follower) = match raft_node {
            Leader(ref mut node) => {
                node.on_request_vote(req, remote_addr)
            }
            Follower(ref mut node) => {
                node.on_request_vote(req, remote_addr)
            }
            Candidate(ref mut node) => {
                node.on_request_vote(req, remote_addr)
            }
        };

        self.0 = Some(
            if to_follower {
                match raft_node {
                    Leader(node) => RaftNode::Follower(node.into()),
                    Follower(node) => RaftNode::Follower(node),
                    Candidate(node) => RaftNode::Follower(node.into())
                }
            } else {
                raft_node
            }
        );

        resp
    }

    fn on_append_entries(&mut self, req: AppendEntriesReq, remote_addr: ServerId) -> AppendEntriesResp {
        let mut raft_node = self.0.take().unwrap();
        use self::RaftNode::*;

        let (resp, to_follower) = match raft_node {
            Leader(ref mut node) => {
                node.on_append_entries(req, remote_addr)
            }
            Follower(ref mut node) => {
                node.on_append_entries(req, remote_addr)
            }
            Candidate(ref mut node) => {
                node.on_append_entries(req, remote_addr)
            }
        };

        self.0 = Some(
            if to_follower {
                match raft_node {
                    Leader(node) => RaftNode::Follower(node.into()),
                    Follower(node) => RaftNode::Follower(node),
                    Candidate(node) => RaftNode::Follower(node.into())
                }
            } else {
                raft_node
            }
        );

        resp
    }

    fn on_update_configuration(&mut self, req: ConfigurationReq, remote_addr: ServerId) -> ConfigurationResp {
        let mut raft_node = self.0.take().unwrap();
        use self::RaftNode::*;

        if let Leader(ref mut node) = raft_node {
            node.on_update_configuration(req, remote_addr);
            ConfigurationResp {
                success: true
            }
        } else {
            ConfigurationResp {
                success: false
            }
        }
    }

    fn on_clock_tick(&mut self) {
        let raft_node = self.0.take().expect("take raft node");
        use self::RaftNode::*;
        self.0 = Some(match raft_node {
            Leader(mut node) => {
                let change_state = node.on_clock_tick();
                assert!(!change_state);
                RaftNode::Leader(node)
            }
            Follower(mut node) => {
                let change_state = node.on_clock_tick();
                if change_state {
                    RaftNode::Candidate(node.into())
                } else {
                    RaftNode::Follower(node)
                }
            }
            Candidate(mut node) => {
                let change_state = node.on_clock_tick();
                if change_state {
                    RaftNode::Leader(node.into())
                } else {
                    RaftNode::Candidate(node)
                }
            }
        });
    }
}
