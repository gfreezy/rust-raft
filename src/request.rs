use std::sync::mpsc::Sender;
use ::rpc::{VoteReq, VoteResp, AppendEntriesReq, AppendEntriesResp, ServerId, CommandReq, CommandResp};


pub struct VoteRequest {
    pub data: VoteReq,
    pub ret: Sender<VoteResp>,
}

pub struct AppendEntriesRequest {
    pub data: AppendEntriesReq,
    pub ret: Sender<AppendEntriesResp>,
}

pub struct TickRequest {
    pub ret: Sender<()>,
}

pub struct CommandRequest {
    pub data: CommandReq,
    pub ret: Sender<CommandResp>,
}

pub enum Request {
    VoteRequest(VoteRequest),
    AppendEntriesRequest(AppendEntriesRequest),
    TickRequest(TickRequest),
    CommandRequest(CommandRequest),
    VoteFor(Vec<(ServerId, VoteReq)>),
    AppendEntriesFor(Vec<(ServerId, AppendEntriesReq)>),
}
