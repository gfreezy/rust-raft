use std::sync::Mutex;
use std::sync::mpsc::{Sender, channel};
use rocket_contrib::json::JSON;
use rocket::{State, Rocket, custom, config};

use ::rpc::{AppendEntriesReq, AppendEntriesResp, VoteReq, VoteResp, CommandReq, CommandResp};
use ::request::{Request, VoteRequest, AppendEntriesRequest, CommandRequest};


pub struct RequestSender(Mutex<Sender<Request>>);

impl RequestSender {
    pub fn new(sender: Sender<Request>) -> Self {
        RequestSender(Mutex::new(sender))
    }
}


#[post("/raft/on_request_vote", data = "<req>")]
fn on_request_vote(req: JSON<VoteReq>, sender: State<RequestSender>) -> JSON<VoteResp> {
    info!("Received Vote <----------------- {:?}", &req);
    let (ret_sender, ret_receiver) = channel::<VoteResp>();
    {
        let sender = sender.0.try_lock().unwrap();
        let req = Request::VoteRequest(VoteRequest {
            data: req.into_inner(),
            ret: ret_sender,
        });
        sender.send(req).expect("send vote req");
    }
    info!("\tReceiving vote resp");
    let resp = ret_receiver.recv().expect("receive vote resp");
    info!("\tFinish request ---------------> {:?}", &resp);
    JSON(resp)
}


#[post("/raft/on_append_entries", data = "<req>")]
fn on_append_entries(req: JSON<AppendEntriesReq>, sender: State<RequestSender>) -> JSON<AppendEntriesResp> {
    let data = req.into_inner();
    let is_heartbeat = data.entries.len() == 0;
    if !is_heartbeat {
        info!("Received entries <----------------- {:?}", &data);
    }
    let (ret_sender, ret_receiver) = channel::<AppendEntriesResp>();
    {
        let sender = sender.0.try_lock().unwrap();
        let req = Request::AppendEntriesRequest(AppendEntriesRequest {
            data: data,
            ret: ret_sender,
        });
        sender.send(req).expect("send append entries req");
    }
    let resp = ret_receiver.recv().expect("receive append entries resp");
    if !is_heartbeat {
        info!("\tFinish request ---------------> {:?}", &resp);
    }
    JSON(resp)
}


#[post("/data/on_receive_command", data = "<req>")]
fn on_receive_command(req: JSON<CommandReq>, sender: State<RequestSender>) -> JSON<CommandResp> {
    info!("Received command <----------------- {:?}", &req);
    let (ret_sender, ret_receiver) = channel::<CommandResp>();
    {
        let sender = sender.0.try_lock().unwrap();
        let req = Request::CommandRequest(CommandRequest{
            data: req.into_inner(),
            ret: ret_sender,
        });
        sender.send(req).expect("send command req");
    }
    let resp = ret_receiver.recv().expect("receive command resp");
    info!("\tFinish request ---------------> {:?}", &resp);
    JSON(resp)
}


pub fn start_rocket(sender: Sender<Request>, port: u16) -> Rocket {
    let c = config::Config::build(config::Environment::Development)
        .port(port)
        .finalize().expect("build config");
    custom(c, false)
        .mount("/", routes![on_request_vote, on_append_entries, on_receive_command])
        .manage(RequestSender::new(sender))
}
