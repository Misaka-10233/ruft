use serde::{Deserialize, Serialize};

// RequestVote RPC 参数：Candidate 发起选举时携带自己的任期和日志进度。
// RequestVote RPC args: candidate sends its term and log progress during election.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestVoteArgs {
    pub(crate) term: u64,           // Candidate 任期。Candidate's term.
    pub(crate) candidate_id: u64,   // Candidate ID。Candidate ID.
    pub(crate) last_log_index: u64, // Candidate 最后一条日志索引。Candidate last log index.
    pub(crate) last_log_term: u64,  // Candidate 最后一条日志任期。Candidate last log term.
}

// RequestVote RPC 响应：包含接收方当前任期和是否投票。
// RequestVote RPC reply: includes receiver's current term and vote decision.
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub(crate) term: u64,          // 接收方当前任期。Receiver's current term.
    pub(crate) vote_granted: bool, // 是否同意投票。Whether the vote is granted.
}
