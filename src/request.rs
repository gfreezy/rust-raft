use std::sync::mpsc::Sender;
use ::rpc::{VoteReq, VoteResp, AppendEntriesReq, AppendEntriesResp, ServerId};


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

pub enum Request {
    VoteRequest(VoteRequest),
    AppendEntriesRequest(AppendEntriesRequest),
    TickRequest(TickRequest),
    VoteFor(ServerId, VoteReq),
    AppendEntriesFor(ServerId, AppendEntriesReq),
}
