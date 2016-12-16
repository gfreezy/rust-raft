use std::sync::{Mutex, Arc};
use ::rpc;
use ::node::{RaftNode, LiveRaftNode};


pub struct RpcServer(Arc<Mutex<Option<RaftNode>>>);

impl RpcServer {
    pub fn new(raft_node: Arc<Mutex<Option<RaftNode>>>) -> Self {
        RpcServer(raft_node)
    }
}

impl rpc::Service for RpcServer {
    fn on_request_vote(&self, req: rpc::VoteReq) -> rpc::VoteResp {
        println!("receive on_request_vote");
        let mut raft_node = self.0.lock().unwrap();
        println!("receive on_request_vote get lock");

        let resp = match *raft_node {
            Some(RaftNode::Follower(ref mut node)) => node.on_request_vote(&req),
            Some(RaftNode::Candidate(ref mut node)) => node.on_request_vote(&req),
            Some(RaftNode::Leader(ref mut node)) => node.on_request_vote(&req),
            None => unreachable!(),
        };
        println!("finish process");
        resp
    }

    fn on_append_entries(&self, req: rpc::AppendEntriesReq) -> rpc::AppendEntriesResp {
        println!("receive on_append_entries");

        let mut raft_node = self.0.lock().unwrap();
        println!("receive on_append_entries get lock");

        match *raft_node {
            Some(RaftNode::Follower(ref mut node)) => node.on_append_entries(&req),
            Some(RaftNode::Candidate(ref mut node)) => node.on_append_entries(&req),
            Some(RaftNode::Leader(ref mut node)) => node.on_append_entries(&req),
            None => unreachable!(),
        }
    }
}
