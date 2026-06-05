#[derive(Debug)]
pub struct RequestVoteArgs {
    pub(crate) term: u64,           // Candidate 任期
    pub(crate) candidate_id: u64,   // Candidate ID
    pub(crate) last_log_index: u64, // Candidate 最后一个日志的索引
    pub(crate) last_log_term: u64,  // Candidate 最后一个日志的任期
}

#[derive(Debug)]
pub struct RequestVoteReply {
    pub(crate) term: u64,          // Candidate 任期
    pub(crate) vote_granted: bool, // 是否投票
}
