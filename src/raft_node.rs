use std::sync::mpsc::Sender;
use std::fmt;

use ::node::{Node, Leader, Follower, Candidate};
use ::rpc::{ServerId, AppendEntriesReq, AppendEntriesResp, VoteReq, VoteResp, CommandResp, CommandReq};
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
                unreachable!()
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
                unreachable!()
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
            _ => unreachable!()
        }
    }
}


pub trait LiveRaftStore {
    fn on_append_entries(&mut self, req: &AppendEntriesReq) -> AppendEntriesResp;
    fn on_request_vote(&mut self, req: &VoteReq) -> VoteResp;
    fn on_clock_tick(&mut self);
}


impl<S: Store> LiveRaftStore for RaftStore<S> {
    fn on_request_vote(&mut self, req: &VoteReq) -> VoteResp {
        use self::RaftNode::*;

        let mut raft_node = self.0.take().unwrap();

        let (resp, to_follower) = match raft_node {
            Leader(ref mut node) => {
                node.on_request_vote(req)
            }
            Follower(ref mut node) => {
                node.on_request_vote(req)
            }
            Candidate(ref mut node) => {
                node.on_request_vote(req)
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

    fn on_append_entries(&mut self, req: &AppendEntriesReq) -> AppendEntriesResp {
        let mut raft_node = self.0.take().unwrap();
        use self::RaftNode::*;

        let (resp, to_follower) = match raft_node {
            Leader(ref mut node) => {
                node.on_append_entries(req)
            }
            Follower(ref mut node) => {
                node.on_append_entries(req)
            }
            Candidate(ref mut node) => {
                node.on_append_entries(req)
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

    fn on_clock_tick(&mut self) {
        let raft_node = self.0.take().expect("take raft node");
        use self::RaftNode::*;
        self.0 = Some(match raft_node {
            Leader(mut node) => {
                let change_state = node.on_clock_tick();
                assert!(change_state == false);
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
