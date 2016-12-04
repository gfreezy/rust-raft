use std::sync::{mpsc, Mutex};
use ::node::{Event, RaftNode};

pub struct RaftServer {
    pub raft_node: Mutex<Option<RaftNode>>,
    noti_center: Mutex<mpsc::Receiver<Event>>,
    to_exit: bool,
}


impl RaftServer {
    pub fn new(server_id: String, servers: &[&str]) -> RaftServer {
        let (sender, receiver) = mpsc::channel();
        RaftServer {
            raft_node: Mutex::new(Some(RaftNode::new(server_id, servers, sender))),
            noti_center: Mutex::new(receiver),
            to_exit: false,
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
                }
            };
            *raft_node = Some(new_raft_node);
        }
    }

    pub fn exit(&mut self) {
        self.to_exit = true;
    }
}
