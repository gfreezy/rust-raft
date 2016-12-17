use std::sync::{Mutex, Arc};
use ::rpc;
use ::raft_node::{RaftNode, LiveRaftNode};


pub struct RpcServer(Arc<Mutex<Option<RaftNode>>>);

impl RpcServer {
    pub fn new(raft_node: Arc<Mutex<Option<RaftNode>>>) -> Self {
        RpcServer(raft_node)
    }
}

impl rpc::Service for RpcServer {
    fn on_request_vote(&self, req: rpc::VoteReq) -> rpc::VoteResp {
        info!("Received Vote <-----------------");
        let mut raft_node = self.0.lock().unwrap();
        info!("\tAquired lock");

        let resp = match *raft_node {
            Some(RaftNode::Follower(ref mut node)) => node.on_request_vote(&req),
            Some(RaftNode::Candidate(ref mut node)) => node.on_request_vote(&req),
            Some(RaftNode::Leader(ref mut node)) => node.on_request_vote(&req),
            None => unreachable!(),
        };
        info!("\tFinish request");
        resp
    }

    fn on_append_entries(&self, req: rpc::AppendEntriesReq) -> rpc::AppendEntriesResp {
        info!("Received Entry <-----------------");

        let mut raft_node = self.0.lock().unwrap();
        info!("\tAquired lock");

        let resp = match *raft_node {
            Some(RaftNode::Follower(ref mut node)) => node.on_append_entries(&req),
            Some(RaftNode::Candidate(ref mut node)) => node.on_append_entries(&req),
            Some(RaftNode::Leader(ref mut node)) => node.on_append_entries(&req),
            None => unreachable!(),
        };
        info!("\tFinish request");
        resp
    }
}
