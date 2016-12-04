use std::collections::HashMap;
use std::sync::{mpsc, Mutex};
use tarpc;

use ::node::{Event, RaftNode};
use ::rpc;

pub struct RaftServer {
    pub raft_node: Mutex<Option<RaftNode>>,
    noti_center: Mutex<mpsc::Receiver<Event>>,
    peers: Mutex<HashMap<String, tarpc::Result<rpc::Client>>>,
    to_exit: bool,
}


impl RaftServer {
    pub fn new(server_id: String, servers: &[&str]) -> RaftServer {
        let (sender, receiver) = mpsc::channel();
        let peers = servers.iter().map(|s| (s.to_string(), rpc::Client::new(*s))).collect();
        RaftServer {
            raft_node: Mutex::new(Some(RaftNode::new(server_id, servers, sender))),
            noti_center: Mutex::new(receiver),
            to_exit: false,
            peers: Mutex::new(peers),
        }
    }

    pub fn run_forever(&self) {
        while !self.to_exit {
            let event = match self.noti_center.lock().unwrap().recv() {
                Ok(e) => e,
                Err(e) => panic!("{:?}", e),
            };

            let mut raft_node = self.raft_node.lock().unwrap();
            let _raft_node = raft_node.take().unwrap();
            let new_raft_node = match event {
                Event::ConvertToFollower => {
                    match _raft_node {
                        RaftNode::Follower(_) => unreachable!(),
                        RaftNode::Candidate(node) => RaftNode::Follower(node.into()),
                        RaftNode::Leader(node) => RaftNode::Follower(node.into()),
                    }
                },
                Event::ConvertToLeader => {
                    match _raft_node {
                        RaftNode::Follower(_) => unreachable!(),
                        RaftNode::Candidate(node) => RaftNode::Leader(node.into()),
                        RaftNode::Leader(_) => unreachable!(),
                    }
                }
                Event::ConvertToCandidate => {
                    match _raft_node {
                        RaftNode::Follower(node) => RaftNode::Candidate(node.into()),
                        RaftNode::Candidate(_) => unreachable!(),
                        RaftNode::Leader(_) => unreachable!(),
                    }
                },
                Event::SendAppendEntries((peer, req)) => {
                    _raft_node
                },
                Event::SendRequestVote((peer, req)) => {
                    _raft_node
                },
            };
            *raft_node = Some(new_raft_node);
        }
    }

    pub fn exit(&mut self) {
        self.to_exit = true;
    }
}
