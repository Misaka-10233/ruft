use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};

#[tarpc::service]
pub trait Rpc {
    async fn append_entries(args: AppendEntriesArgs) -> AppendEntriesReply;
    async fn request_vote(args: RequestVoteArgs) -> RequestVoteReply;
}
