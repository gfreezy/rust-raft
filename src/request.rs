use std::sync::mpsc::Sender;
use ::rpc::{VoteReq, VoteResp, AppendEntriesReq, AppendEntriesResp, ServerId, CommandReq, CommandResp, ConfigurationReq, ConfigurationResp};


pub struct VoteRequest {
    pub data: VoteReq,
    pub remote_addr: ServerId,
    pub ret: Sender<VoteResp>,
}

pub struct AppendEntriesRequest {
    pub data: AppendEntriesReq,
    pub remote_addr: ServerId,
    pub ret: Sender<AppendEntriesResp>,
}

pub struct TickRequest {
    pub ret: Sender<()>,
}

pub struct CommandRequest {
    pub data: CommandReq,
    pub remote_addr: ServerId,
    pub ret: Sender<CommandResp>,
}


pub struct UpdateConfigurationRequest {
    pub data: ConfigurationReq,
    pub remote_addr: ServerId,
    pub ret: Sender<ConfigurationResp>,
}


pub enum Request {
    VoteRequest(VoteRequest),
    AppendEntriesRequest(AppendEntriesRequest),
    TickRequest(TickRequest),
    CommandRequest(CommandRequest),
    UpdateConfiguration(UpdateConfigurationRequest),
    VoteFor(Vec<(ServerId, VoteReq)>),
    AppendEntriesFor(Vec<(ServerId, AppendEntriesReq)>),
}
