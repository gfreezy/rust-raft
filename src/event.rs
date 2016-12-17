use ::rpc::{VoteReq, AppendEntriesReq};


#[derive(Debug)]
pub enum Event {
    ClockTick,
    ConvertToFollower,
    ConvertToLeader,
    ConvertToCandidate,
    SendRequestVote((String, VoteReq)),
    SendAppendEntries((String, AppendEntriesReq)),
}
