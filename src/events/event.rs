use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};
use tokio::sync::oneshot;

pub enum Event {
    AppendEntries(AppendEntriesArgs, oneshot::Sender<AppendEntriesReply>),
    RequestVote(RequestVoteArgs, oneshot::Sender<RequestVoteReply>),
    Apply,
    ElectionTimeout,
    VoteResult(bool, u64),
    ReceiveEnoughVotes,
    ShouldBeFollower(u64),
    Heartbeat,
    AppendEntriesReply(AppendEntriesReply),
    NewLogEntries(Vec<u8>),
}
