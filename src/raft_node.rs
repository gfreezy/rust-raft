use std::sync::mpsc::Sender;
use std::fmt;

use ::node::{Node, Leader, Follower, Candidate};
use ::event::Event;
use ::rpc::{ServerId, AppendEntriesReq, AppendEntriesResp, VoteReq, VoteResp};
use ::store::Store;

#[derive(Debug)]
pub enum RaftNode<S: Store> {
    Leader(Node<Leader, S>),
    Follower(Node<Follower, S>),
    Candidate(Node<Candidate, S>),
}


impl<S: Store> RaftNode<S> {
    pub fn new(server_id: ServerId, store: S, servers: Vec<ServerId>, noti_center: Sender<Event>) -> Self {
        RaftNode::Follower(Node::new(server_id, store, servers, noti_center))
    }
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


pub trait LiveRaftNode {
    fn on_append_entries(&mut self, req: &AppendEntriesReq) -> AppendEntriesResp;
    fn on_request_vote(&mut self, req: &VoteReq) -> VoteResp;
    fn on_clock_tick(&mut self);
}
