use std::sync::Mutex;
use std::net::SocketAddr;
use std::sync::mpsc::{Sender, channel};
use rocket_contrib::Json;
use rocket::{State, Rocket, custom, config};

use ::rpc::{AppendEntriesReq, AppendEntriesResp, VoteReq, VoteResp, CommandReq, CommandResp, ServerId, ConfigurationReq, ConfigurationResp};
use ::request::{Request, VoteRequest, AppendEntriesRequest, CommandRequest, UpdateConfigurationRequest};


pub struct RequestSender(Mutex<Sender<Request>>);

impl RequestSender {
    pub fn new(sender: Sender<Request>) -> Self {
        RequestSender(Mutex::new(sender))
    }
}


#[post("/raft/on_request_vote", data = "<vote_req>")]
fn on_request_vote(vote_req: Json<VoteReq>, sender: State<RequestSender>, remote_addr: SocketAddr) -> Json<VoteResp> {
    info!("Received Vote <----------------- {:?}", &vote_req);
    let (ret_sender, ret_receiver) = channel::<VoteResp>();
    {
        let sender = sender.0.try_lock().unwrap();
        let req = Request::VoteRequest(VoteRequest {
            data: vote_req.into_inner(),
            remote_addr: ServerId(format!("{}:{}", remote_addr.ip(), remote_addr.port())),
            ret: ret_sender,
        });
        sender.send(req).expect("send vote req");
    }
    info!("\tReceiving vote resp");
    let resp = ret_receiver.recv().expect("receive vote resp");
    info!("\tFinish request ---------------> {:?}", &resp);
    Json(resp)
}


#[post("/raft/on_append_entries", data = "<req>")]
fn on_append_entries(req: Json<AppendEntriesReq>, sender: State<RequestSender>, remote_addr: SocketAddr) -> Json<AppendEntriesResp> {
    let data = req.into_inner();
    let is_heartbeat = data.entries.is_empty();
    if !is_heartbeat {
        info!("Received entries <----------------- {:?}", &data);
    }
    let (ret_sender, ret_receiver) = channel::<AppendEntriesResp>();
    {
        let sender = sender.0.try_lock().unwrap();
        let req = Request::AppendEntriesRequest(AppendEntriesRequest {
            data,
            remote_addr: ServerId(format!("{}:{}", remote_addr.ip(), remote_addr.port())),
            ret: ret_sender,
        });
        sender.send(req).expect("send append entries req");
    }
    let resp = ret_receiver.recv().expect("receive append entries resp");
    if !is_heartbeat {
        info!("\tFinish request ---------------> {:?}", &resp);
    }
    Json(resp)
}


#[post("/raft/on_update_configuration", data = "<req>")]
fn on_update_config(req: Json<ConfigurationReq>, sender: State<RequestSender>, remote_addr: SocketAddr) -> Json<ConfigurationResp> {
    let data = req.into_inner();
    let (ret_sender, ret_receiver) = channel::<ConfigurationResp>();
    {
        let sender = sender.0.try_lock().unwrap();
        let req = Request::UpdateConfiguration(UpdateConfigurationRequest{
            data,
            remote_addr: ServerId(format!("{}:{}", remote_addr.ip(), remote_addr.port())),
            ret: ret_sender,
        });
        sender.send(req).expect("send update configuration req");
    }
    let resp = ret_receiver.recv().expect("receive update configuration resp");
    Json(resp)
}


#[post("/data/on_receive_command", data = "<req>")]
fn on_receive_command(req: Json<CommandReq>, sender: State<RequestSender>, remote_addr: SocketAddr) -> Json<CommandResp> {
    info!("Received command <----------------- {:?}", &req);
    let (ret_sender, ret_receiver) = channel::<CommandResp>();
    {
        let sender = sender.0.try_lock().unwrap();
        let req = Request::CommandRequest(CommandRequest{
            data: req.into_inner(),
            remote_addr: ServerId(format!("{}:{}", remote_addr.ip(), remote_addr.port())),
            ret: ret_sender,
        });
        sender.send(req).expect("send command req");
    }
    let resp = ret_receiver.recv().expect("receive command resp");
    info!("\tFinish request ---------------> {:?}", &resp);
    Json(resp)
}



pub fn start_rocket(sender: Sender<Request>, port: u16) -> Rocket {
    let c = config::Config::build(config::Environment::Development)
        .port(port)
        .finalize().expect("build config");
    custom(c, false)
        .mount("/", routes![on_request_vote, on_append_entries, on_receive_command, on_update_config])
        .manage(RequestSender::new(sender))
}
