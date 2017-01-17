use std::sync::{Mutex, Arc};
use ::rpc;
use ::raft_node::{RaftNode, LiveRaftNode};
use ::store::Store;

pub struct RpcServer<S: Store + 'static>(Arc<Mutex<Option<RaftNode<S>>>>);

impl<S: Store + 'static> RpcServer<S> {
    pub fn new(raft_node: Arc<Mutex<Option<RaftNode<S>>>>) -> Self {
        RpcServer(raft_node)
    }
}

impl<S: Store + 'static> rpc::Service for RpcServer<S> {
    fn on_request_vote(&self, req: rpc::VoteReq) -> rpc::VoteResp {
        info!("Received Vote <----------------- {:?}", &req);
        let mut raft_node = self.0.lock().unwrap();
        info!("\tAquired lock");

        let resp = match *raft_node {
            Some(RaftNode::Follower(ref mut node)) => node.on_request_vote(&req),
            Some(RaftNode::Candidate(ref mut node)) => node.on_request_vote(&req),
            Some(RaftNode::Leader(ref mut node)) => node.on_request_vote(&req),
            None => unreachable!(),
        };
        info!("\tFinish request ---------------> {:?}", &resp);
        resp
    }

    fn on_append_entries(&self, req: rpc::AppendEntriesReq) -> rpc::AppendEntriesResp {
        info!("Received Entry <----------------- {:?}", &req);

        let mut raft_node = self.0.lock().unwrap();
        info!("\tAquired lock");

        let resp = match *raft_node {
            Some(RaftNode::Follower(ref mut node)) => node.on_append_entries(&req),
            Some(RaftNode::Candidate(ref mut node)) => node.on_append_entries(&req),
            Some(RaftNode::Leader(ref mut node)) => node.on_append_entries(&req),
            None => unreachable!(),
        };
        info!("\tFinish request ---------------> {:?}", &resp);

        resp
    }
}
