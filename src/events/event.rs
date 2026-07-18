use crate::result::AppendResult;
use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::install_snapshot::{InstallSnapshotArgs, InstallSnapshotReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};
use crate::ruft::RuftInfo;
use tokio::sync::oneshot;

// 节点内部事件：把 RPC、计时器和本地命令统一串到单线程状态机中。
pub enum Event {
    // 收到 Leader 的日志复制或心跳请求，并通过 oneshot 返回响应。
    AppendEntries(AppendEntriesArgs, oneshot::Sender<AppendEntriesReply>),
    // 收到 Candidate 的投票请求，并通过 oneshot 返回响应。
    RequestVote(RequestVoteArgs, oneshot::Sender<RequestVoteReply>),
    // 收到 Leader 的快照安装请求，并通过 oneshot 返回响应。
    InstallSnapshot(InstallSnapshotArgs, oneshot::Sender<InstallSnapshotReply>),
    // 选举超时，Follower/Candidate 会尝试发起新一轮选举。
    ElectionTimeout,
    // 投票 RPC 的聚合结果；new term 用于发现更高任期时退回 Follower。
    VoteResult(bool, u64),
    // 当前 Candidate 已获得多数票，可以晋升为 Leader。
    ReceiveEnoughVotes,
    // 发现更高任期或合法 Leader，当前节点应转为 Follower。
    ShouldBeFollower(u64),
    // 周期性心跳触发，只有 Leader 会真正广播 AppendEntries。
    Heartbeat,
    // Follower 对日志复制的响应，用于推进或回退复制进度。
    AppendEntriesReply(AppendEntriesReply),
    // Follower 对快照安装的响应，用于推进复制进度或退位。
    InstallSnapshotReply(InstallSnapshotReply),
    // 客户端提交的新命令，只有 Leader 会追加到日志。
    NewLogEntries(Vec<u8>, oneshot::Sender<AppendResult>),
    // 上层状态机提交快照数据，请求 Raft 压缩已经应用的日志前缀。
    CreateSnapshot(u64, Vec<u8>, oneshot::Sender<bool>),
    // 读取当前节点状态快照，供外部监控和集成测试观测。
    GetInfo(oneshot::Sender<RuftInfo>),
}
