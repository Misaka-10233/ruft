use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::install_snapshot::{InstallSnapshotArgs, InstallSnapshotReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};

// 节点对外暴露的 Raft RPC 接口，由 tarpc 生成客户端和服务端胶水代码。
#[tarpc::service]
pub trait Rpc {
    // Leader 用于心跳和日志复制。
    async fn append_entries(args: AppendEntriesArgs) -> AppendEntriesReply;
    // Candidate 用于拉票。
    async fn request_vote(args: RequestVoteArgs) -> RequestVoteReply;
    // Leader 用于向落后于压缩边界的 Follower 安装快照。
    async fn install_snapshot(args: InstallSnapshotArgs) -> InstallSnapshotReply;
}
