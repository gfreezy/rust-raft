use std::fmt;


#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
pub struct ServerId(pub String);

impl ServerId {
    pub fn addr(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, buf: &mut fmt::Formatter) -> fmt::Result {
        write!(buf, "{}", self.0)
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteReq {
    pub term: u64,
    pub candidate_id: ServerId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteResp {
    pub term: u64,
    pub vote_granted: bool,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    pub term: u64,
    pub payload: String,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesReq {
    pub term: u64,
    pub leader_id: ServerId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<Entry>,
    pub leader_commit: u64,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesResp {
    pub term: u64,
    pub success: bool,
}


service! {
    rpc on_request_vote(req: VoteReq) -> VoteResp;
    rpc on_append_entries(req: AppendEntriesReq) -> AppendEntriesResp;
}
