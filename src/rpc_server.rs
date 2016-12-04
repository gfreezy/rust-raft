use std::sync::Arc;
use ::raft_server::RaftServer;
use ::rpc;
use ::node::{RaftNode, LiveRaftNode};


pub struct RpcServer(pub Arc<RaftServer>);

impl rpc::Service for RpcServer {
    fn on_request_vote(&self, req: rpc::VoteReq) -> rpc::VoteResp {
        let mut raft_node = self.0.raft_node.lock().unwrap();
        match *raft_node {
            Some(RaftNode::Follower(ref mut node)) => node.on_request_vote(&req),
            Some(RaftNode::Candidate(ref mut node)) => node.on_request_vote(&req),
            Some(RaftNode::Leader(ref mut node)) => node.on_request_vote(&req),
            None => unreachable!(),
        }
    }

    fn on_append_entries(&self, req: rpc::AppendEntriesReq) -> rpc::AppendEntriesResp {
        let mut raft_node = self.0.raft_node.lock().unwrap();
        match *raft_node {
            Some(RaftNode::Follower(ref mut node)) => node.on_append_entries(&req),
            Some(RaftNode::Candidate(ref mut node)) => node.on_append_entries(&req),
            Some(RaftNode::Leader(ref mut node)) => node.on_append_entries(&req),
            None => unreachable!(),
        }
    }
}
