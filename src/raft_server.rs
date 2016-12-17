use std::collections::HashMap;
use std::sync::{Mutex, Arc};
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;
use tarpc;
use timer;
use time;

use ::raft_node::{RaftNode, LiveRaftNode};
use ::event::Event;
use ::rpc;
use ::rpc_server;
use ::rpc::Service;

pub struct RaftServer {
    pub raft_node: Arc<Mutex<Option<RaftNode>>>,
    noti_center: Receiver<Event>,
    peers: HashMap<String, tarpc::Result<rpc::Client>>,
    server_handle: tarpc::ServeHandle,
    timer: timer::Timer,
    guard: timer::Guard,
}


impl RaftServer {
    pub fn new(addr: String, servers: &[&str]) -> RaftServer {
        let (sender, receiver) = channel();
        let peers = servers.iter().map(|s| (s.to_string(), rpc::Client::new(*s))).collect();
        let raft_node = Arc::new(Mutex::new(Some(RaftNode::new(addr.clone(), servers, sender.clone()))));
        let s = rpc_server::RpcServer::new(raft_node.clone());
        let server_handle = s.spawn_with_config(addr.as_str(), tarpc::Config { timeout: Some(Duration::new(5, 0)) }).expect("listen");

        let timer = timer::Timer::new();
        let guard = timer.schedule_repeating(time::Duration::milliseconds(100), move || {
            sender.send(Event::ClockTick).expect("send event");
        });

        RaftServer {
            raft_node: raft_node,
            noti_center: receiver,
            peers: peers,
            server_handle: server_handle,
            timer: timer,
            guard: guard,
        }
    }

    pub fn run_forever(&self) {
        let mut pool = ConnectionPool::new(self.peers.keys().cloned().collect());
        let noti_center = &self.noti_center;
        loop {
            let event = noti_center.recv();
            match event {
                Err(e) => {
                    error!("{:?}", e);
                },
                Ok(Event::ClockTick) => {
                    let mut raft_node = self.raft_node.lock().expect("lock raft node");
                    let _raft_node = raft_node.take().unwrap();
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
                    });
                },
                Ok(Event::ConvertToFollower) => {
                    let mut raft_node = self.raft_node.lock().expect("lock raft node");
                    let _raft_node = raft_node.take().unwrap();

                    info!("-------------ConvertToFollower-------------");

                    *raft_node = Some(match _raft_node {
                        RaftNode::Follower(_) => unreachable!(),
                        RaftNode::Candidate(node) => RaftNode::Follower(node.into()),
                        RaftNode::Leader(node) => RaftNode::Follower(node.into()),
                    });
                },
                Ok(Event::ConvertToLeader) => {
                    let mut raft_node = self.raft_node.lock().expect("lock raft node");
                    let _raft_node = raft_node.take().unwrap();
                    info!("+++++++++++++ConvertToLeader+++++++++++++");

                    *raft_node = Some(match _raft_node {
                        RaftNode::Follower(_) => unreachable!(),
                        RaftNode::Candidate(node) => RaftNode::Leader(node.into()),
                        RaftNode::Leader(_) => unreachable!(),
                    });
                }
                Ok(Event::ConvertToCandidate) => {
                    let mut raft_node = self.raft_node.lock().expect("lock raft node");
                    let _raft_node = raft_node.take().unwrap();
                    info!("#############ConvertToCandidate#############");

                    *raft_node = Some(match _raft_node {
                        RaftNode::Follower(node) => RaftNode::Candidate(node.into()),
                        RaftNode::Candidate(_) => unreachable!(),
                        RaftNode::Leader(_) => unreachable!(),
                    });
                },
                Ok(Event::SendAppendEntries((peer, req))) => {
                    {
                        let raft_node = self.raft_node.lock().expect("lock raft node");
                        match *raft_node {
                            Some(RaftNode::Follower(_)) | Some(RaftNode::Candidate(_)) => {
                                continue;
                            },
                            _ => {}
                        };
                    }

                    info!("Entry: --------> {}", &peer);
                    let ret = pool.get_client(&peer).and_then(|c| {
                        info!("\tSending request");
                        let resp = c.on_append_entries(req);
                        info!("\tRecevied Response");
                        Some(match resp {
                            Ok(r) => {
                                info!("\tLocking raft node");
                                let mut raft_node = self.raft_node.lock().expect("lock raft node");
                                info!("\tAquired raft node lock");

                                if let Some(RaftNode::Leader(ref mut node)) = *raft_node {
                                    info!("\tProcess response");
                                    node.on_receive_append_entries_request(&peer, r);
                                }
                                info!("\tFinished process response");

                                Ok(())
                            },
                            Err(e) => {
                                info!("Error: {:?}", &e);
                                Err(e)
                            },
                        })
                    });
                    match ret {
                        Some(Ok(_)) => {},
                        _ => {
                            info!("\tRemoved client");
                            pool.remove_client(&peer);
                        },
                    };
                },
                Ok(Event::SendRequestVote((peer, req))) => {
                    {
                        let raft_node = self.raft_node.lock().expect("lock raft node");
                        match *raft_node {
                            Some(RaftNode::Follower(_)) | Some(RaftNode::Leader(_)) => {
                                continue;
                            },
                            _ => {}
                        };
                    }

                    info!("Vote: --------> {}", &peer);
                    let ret = pool.get_client(&peer).and_then(|c| {
                        info!("\tSending request");
                        let resp = c.on_request_vote(req);
                        info!("\tRecevied Response");
                        Some(match resp {
                            Ok(r) => {
                                info!("\tLocking raft node");
                                let mut raft_node = self.raft_node.lock().expect("lock raft node");
                                info!("\tAquired raft node lock");

                                if let Some(RaftNode::Candidate(ref mut node)) = *raft_node {
                                    node.on_receive_vote_request(&peer, r);
                                }
                                info!("\tFinished process response");

                                Ok(())
                            },
                            Err(e) => {
                                info!("Error: {:?}", &e);
                                Err(e)
                            },
                        })
                    });
                    match ret {
                        Some(Ok(_)) => {},
                        _ => {
                            info!("\tRemoved client");
                            pool.remove_client(&peer);
                        },
                    };
                }
            };
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
            let c = rpc::Client::new(addr);
            map.insert(addr.to_string(), c);
        }
        ConnectionPool {
            conns: map,
        }
    }

    pub fn get_client(&mut self, addr: &str) -> Option<&rpc::Client> {
        let v = self.conns.entry(addr.to_string()).or_insert_with(||rpc::Client::new(addr));
        if v.is_ok() {
            return v.as_ref().ok();
        }
        let c = rpc::Client::with_config(addr, tarpc::Config { timeout: Some(Duration::new(5, 0)) });
        *v = c;
        v.as_ref().ok()
    }

    pub fn remove_client(&mut self, addr: &str) {
        self.conns.remove(addr);
    }
}
