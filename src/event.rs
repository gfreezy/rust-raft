use ::rpc::{VoteReq, AppendEntriesReq, ServerId};


#[derive(Debug)]
pub enum Event {
    ClockTick,
    ConvertToFollower,
    ConvertToLeader,
    ConvertToCandidate,
    SendRequestVote((ServerId, VoteReq)),
    SendAppendEntries((ServerId, AppendEntriesReq)),
}
