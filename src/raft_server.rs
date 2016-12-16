use std::collections::HashMap;
use std::sync::{Mutex, Arc};
use std::time::Duration;
use tarpc;
use chan;

use ::node::{Event, RaftNode};
use ::rpc;
use ::rpc_server;
use ::rpc::Service;
use node::LiveRaftNode;

pub struct RaftServer {
    pub raft_node: Arc<Mutex<Option<RaftNode>>>,
    noti_center: chan::Receiver<Event>,
    peers: HashMap<String, tarpc::Result<rpc::Client>>,
    server_handle: tarpc::ServeHandle,
}


impl RaftServer {
    pub fn new(addr: String, servers: &[&str]) -> RaftServer {
        let (sender, receiver) = chan::async();
        let peers = servers.iter().map(|s| (s.to_string(), rpc::Client::new(*s))).collect();
        let raft_node = Arc::new(Mutex::new(Some(RaftNode::new(addr.clone(), servers, sender))));
        let s = rpc_server::RpcServer::new(raft_node.clone());
        let server_handle = s.spawn_with_config(addr.as_str(), tarpc::Config{ timeout: Some(Duration::new(5, 0))}).expect("listen");

        RaftServer {
            raft_node: raft_node,
            noti_center: receiver,
            peers: peers,
            server_handle: server_handle,
        }
    }

    pub fn run_forever(&self) {
        let mut pool = ConnectionPool::new(self.peers.keys().cloned().collect());
        let tick = chan::tick_ms(100);
        let noti_center = &self.noti_center;
        loop {
            chan_select! {
                noti_center.recv() -> event => {{
                    let mut raft_node = self.raft_node.lock().expect("lock raft node");
                    let _raft_node = raft_node.take().unwrap();
                    debug!("{} recv {:?}", _raft_node, event);
                    let new_raft_node = match event {
                        Some(Event::ConvertToFollower) => {
                            match _raft_node {
                                RaftNode::Follower(_) => unreachable!(),
                                RaftNode::Candidate(node) => RaftNode::Follower(node.into()),
                                RaftNode::Leader(node) => RaftNode::Follower(node.into()),
                            }
                        },
                        Some(Event::ConvertToLeader) => {
                            match _raft_node {
                                RaftNode::Follower(_) => unreachable!(),
                                RaftNode::Candidate(node) => RaftNode::Leader(node.into()),
                                RaftNode::Leader(_) => unreachable!(),
                            }
                        }
                        Some(Event::ConvertToCandidate) => {
                            match _raft_node {
                                RaftNode::Follower(node) => RaftNode::Candidate(node.into()),
                                RaftNode::Candidate(_) => unreachable!(),
                                RaftNode::Leader(_) => unreachable!(),
                            }
                        },
                        Some(Event::SendAppendEntries((peer, req))) => {
                            match _raft_node {
                                RaftNode::Follower(node) => {
                                    RaftNode::Follower(node)
                                },
                                RaftNode::Candidate(node) => {
                                    RaftNode::Candidate(node)
                                }
                                RaftNode::Leader(mut node) => {
                                    debug!("send append entry: {}", &peer);
                                    let ret = pool.get_client(&peer).and_then(|c| {
                                        debug!("send append entry get client: {}", &peer);
                                        let resp = c.on_append_entries(req);
                                        debug!("send append entry req: {}", &peer);
                                        Some(match resp {
                                            Ok(r) => {
                                                debug!("before on_receive_append_entries_request: {}", &peer);
                                                node.on_receive_append_entries_request(&peer, r);
                                                Ok(())
                                            },
                                            Err(e) => {
                                                debug!("send to {} error: {:?}", &peer, &e);
                                                Err(e)
                                            },
                                        })
                                    });
                                    match ret {
                                        Some(Err(tarpc::Error::ConnectionBroken)) => {
                                            pool.remove_client(&peer);
                                        },
                                        _ => (),
                                    };


//                                    if let Some(c) = pool.get_client(&peer) {
//                                        let resp = c.on_append_entries(req);
//                                        node.on_receive_append_entries_request(&peer, resp.expect("append entries"));
//                                    }
                                    RaftNode::Leader(node)
                                },
                            }
                        },
                        Some(Event::SendRequestVote((peer, req))) => {
                            match _raft_node {
                                RaftNode::Follower(node) => {
                                    RaftNode::Follower(node)
                                },
                                RaftNode::Candidate(mut node) => {
                                    debug!("send request vote: {}", &peer);
                                    let ret = pool.get_client(&peer).and_then(|c| {
                                        debug!("on_receive_vote_request get client: {}", &peer);
                                        let resp = c.on_request_vote(req);
                                        debug!("on_receive_vote_request send req: {}", &peer);
                                        Some(match resp {
                                            Ok(r) => {
                                                debug!("before on_receive_vote_request: {}", &peer);
                                                node.on_receive_vote_request(&peer, r);
                                                Ok(())
                                            },
                                            Err(e) => {
                                                debug!("send to {} error: {:?}", &peer, &e);
                                                Err(e)
                                            },
                                        })
                                    });
                                    match ret {
                                        Some(Err(tarpc::Error::ConnectionBroken)) => {
                                            pool.remove_client(&peer);
                                        },
                                        _ => (),
                                    };
                                    RaftNode::Candidate(node)
                                },
                                RaftNode::Leader(node) => {
                                    RaftNode::Leader(node)
                                }
                            }
                        },
                        None => _raft_node,
                    };
                    *raft_node = Some(new_raft_node);
                }},
                tick.recv() => {{
                    debug!("tick");
                    let mut raft_node = self.raft_node.lock().expect("lock");
                    let _raft_node = raft_node.take().expect("take");
                    *raft_node = Some(match _raft_node {
                        RaftNode::Follower(mut node) => {
                            node.on_clock_tick();
                            RaftNode::Follower(node)
                        },
                        RaftNode::Candidate(mut node) => {
                            node.on_clock_tick();
                            RaftNode::Candidate(node)
                        }
                        RaftNode::Leader(mut node) => {
                            node.on_clock_tick();
                            RaftNode::Leader(node)
                        }
                    })
                }},
            }
        }
    }
}


struct ConnectionPool {
    conns: HashMap<String, tarpc::Result<rpc::Client>>,
}

impl ConnectionPool {
    pub fn new(addrs: Vec<String>) -> Self {
        let mut map = HashMap::new();
        for addr in &addrs {
            let c = rpc::AsyncClient::new(addr);
            map.insert(addr.to_string(), c);
        }
        ConnectionPool {
            conns: map,
        }
    }

    pub fn get_client(&mut self, addr: &str) -> Option<&rpc::AsyncClient> {
        let v = self.conns.entry(addr.to_string()).or_insert(rpc::AsyncClient::new(addr));
        if v.is_ok() {
            return v.as_ref().ok();
        }
        let c = rpc::AsyncClient::new(addr);
        *v = c;
        v.as_ref().ok()
    }

    pub fn remove_client(&mut self, addr: &str) {
        self.conns.remove(addr);
    }
}
