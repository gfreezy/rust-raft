use std::sync::{Mutex, Arc};
use std::sync::mpsc::{Sender, channel};
use ::rpc::{AppendEntriesReq, AppendEntriesResp, VoteReq, VoteResp, Service};
use ::request::{Request, VoteRequest, AppendEntriesRequest};


pub struct RpcServer(Arc<Mutex<Sender<Request>>>);


impl RpcServer {
    pub fn new(sender: Arc<Mutex<Sender<Request>>>) -> Self {
        RpcServer(sender)
    }
}


impl Service for RpcServer {
    fn on_request_vote(&self, req: VoteReq) -> VoteResp {
        info!("Received Vote <----------------- {:?}", &req);
        let (ret_sender, ret_receiver) = channel::<VoteResp>();
        {
            let sender = self.0.try_lock().unwrap();
            let req = Request::VoteRequest(VoteRequest {
                data: req,
                ret: ret_sender,
            });
            sender.send(req).expect("send vote req");
        }
        info!("\tReceiving vote resp");
        let resp = ret_receiver.recv().expect("receive vote resp");
        info!("\tFinish request ---------------> {:?}", &resp);
        resp
    }

    fn on_append_entries(&self, req: AppendEntriesReq) -> AppendEntriesResp {
        info!("Received entries <----------------- {:?}", &req);
        let (ret_sender, ret_receiver) = channel::<AppendEntriesResp>();
        {
            let sender = self.0.try_lock().unwrap();
            let req = Request::AppendEntriesRequest(AppendEntriesRequest {
                data: req,
                ret: ret_sender,
            });
            sender.send(req).expect("send append entries req");
        }
        let resp = ret_receiver.recv().expect("receive append entries resp");
        info!("\tFinish request ---------------> {:?}", &resp);
        resp
    }
}
