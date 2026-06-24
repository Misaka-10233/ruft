use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};

// 节点对外暴露的 Raft RPC 接口，由 tarpc 生成客户端和服务端胶水代码。
// Public Raft RPC surface; tarpc generates client/server glue from this trait.
#[tarpc::service]
pub trait Rpc {
    // Leader 用于心跳和日志复制。
    // Used by leaders for heartbeats and log replication.
    async fn append_entries(args: AppendEntriesArgs) -> AppendEntriesReply;
    // Candidate 用于拉票。
    // Used by candidates to request votes.
    async fn request_vote(args: RequestVoteArgs) -> RequestVoteReply;
}
