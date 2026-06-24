use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};
use tokio::sync::oneshot;

// 节点内部事件：把 RPC、计时器和本地命令统一串到单线程状态机中。
// Internal node events: funnel RPCs, timers, and local commands into one state machine.
pub enum Event {
    // 收到 Leader 的日志复制或心跳请求，并通过 oneshot 返回响应。
    // Received AppendEntries/heartbeat from the leader; reply through oneshot.
    AppendEntries(AppendEntriesArgs, oneshot::Sender<AppendEntriesReply>),
    // 收到 Candidate 的投票请求，并通过 oneshot 返回响应。
    // Received RequestVote from a candidate; reply through oneshot.
    RequestVote(RequestVoteArgs, oneshot::Sender<RequestVoteReply>),
    // 将已提交但尚未应用的日志推送给上层状态机。
    // Apply committed but not-yet-applied log entries to the upper state machine.
    Apply,
    // 选举超时，Follower/Candidate 会尝试发起新一轮选举。
    // Election timeout; follower/candidate may start a new election round.
    ElectionTimeout,
    // 投票 RPC 的聚合结果；new term 用于发现更高任期时退回 Follower。
    // Aggregated vote result; new term steps down on discovering a higher term.
    VoteResult(bool, u64),
    // 当前 Candidate 已获得多数票，可以晋升为 Leader。
    // Current candidate has a majority and may become leader.
    ReceiveEnoughVotes,
    // 发现更高任期或合法 Leader，当前节点应转为 Follower。
    // A higher term or valid leader was observed; step down to follower.
    ShouldBeFollower(u64),
    // 周期性心跳触发，只有 Leader 会真正广播 AppendEntries。
    // Periodic heartbeat trigger; only leaders broadcast AppendEntries.
    Heartbeat,
    // Follower 对日志复制的响应，用于推进或回退复制进度。
    // AppendEntries response from a follower; advances or backs off replication.
    AppendEntriesReply(AppendEntriesReply),
    // 客户端提交的新命令，只有 Leader 会追加到日志。
    // New client command; only the leader appends it to the log.
    NewLogEntries(Vec<u8>),
}
