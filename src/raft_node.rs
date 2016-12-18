use std::sync::mpsc::Sender;
use std::fmt;

use ::node::{Node, Leader, Follower, Candidate};
use ::event::Event;
use ::rpc::{ServerId, AppendEntriesReq, AppendEntriesResp, VoteReq, VoteResp};


#[derive(Debug)]
pub enum RaftNode {
    Leader(Node<Leader>),
    Follower(Node<Follower>),
    Candidate(Node<Candidate>),
}


impl RaftNode {
    pub fn new(server_id: ServerId, servers: Vec<ServerId>, noti_center: Sender<Event>) -> Self {
        RaftNode::Follower(Node::new(server_id, servers, noti_center))
    }
}


impl fmt::Display for RaftNode {
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
