use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};
use tokio::sync::oneshot;

pub enum Event {
    AppendEntries(AppendEntriesArgs, oneshot::Sender<AppendEntriesReply>),
    RequestVote(RequestVoteArgs, oneshot::Sender<RequestVoteReply>),
}
