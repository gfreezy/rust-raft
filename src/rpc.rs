use std;
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Clone, PartialOrd, Copy)]
pub struct Term(pub u64);

impl Term {
    pub fn incr(&mut self) {
        self.0 += 1;
    }
}

impl fmt::Display for Term {
    fn fmt(&self, buf: &mut fmt::Formatter) -> fmt::Result {
        write!(buf, "{}", self.0)
    }
}


#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Hash, Clone, Copy)]
pub struct EntryIndex(pub u64);

impl EntryIndex {
    pub fn incr(&mut self) {
        self.0 += 1;
    }

    pub fn prev_or_zero(&self) -> EntryIndex {
        if self.0 >= 1 {
            EntryIndex(self.0 - 1)
        } else {
            EntryIndex(0)
        }
    }
}

impl fmt::Display for EntryIndex {
    fn fmt(&self, buf: &mut fmt::Formatter) -> fmt::Result {
        write!(buf, "{}", self.0)
    }
}

impl std::ops::Add<usize> for EntryIndex {
    type Output = EntryIndex;
    fn add(self, rhs: usize) -> Self::Output {
        EntryIndex(self.0 + rhs as u64)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteReq {
    pub term: Term,
    pub candidate_id: ServerId,
    pub last_log_index: EntryIndex,
    pub last_log_term: Term,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteResp {
    pub term: Term,
    pub vote_granted: bool,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    pub term: Term,
    pub payload: String,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesReq {
    pub term: Term,
    pub leader_id: ServerId,
    pub prev_log_index: EntryIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Entry>,
    pub leader_commit: EntryIndex,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
pub struct CommandReq(pub String);


#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
pub struct CommandResp(pub String);
