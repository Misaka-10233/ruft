use crate::ruft::LogEntry;
use serde::{Deserialize, Serialize};

// AppendEntries RPC 参数：Leader 用它复制日志，也用空 entries 发送心跳。
// AppendEntries RPC args: leaders replicate logs with it, or send heartbeats with empty entries.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppendEntriesArgs {
    pub(crate) term: u64,           // Leader 任期。Leader's term.
    pub(crate) leader_id: u64,      // Leader ID。Leader ID.
    pub(crate) prev_log_index: u64, // 新条目前一条日志索引。Index before new entries.

    pub(crate) prev_log_term: u64, // 新条目前一条日志任期。Term before new entries.
    pub(crate) entries: Vec<LogEntry>, // 待复制的新日志条目。Entries to replicate.

    pub(crate) leader_commit: u64, // Leader 已提交索引。Leader commit index.
}

// AppendEntries RPC 响应：Follower 告诉 Leader 当前任期和复制是否成功。
// AppendEntries RPC reply: follower reports its term, match result, and replicated index.
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub(crate) node_id: u64,     // 响应节点 ID。Replying node ID.
    pub(crate) term: u64,        // 响应节点当前任期。Replying node's current term.
    pub(crate) success: bool,    // 日志匹配并写入成功。Log matched and appended.
    pub(crate) match_index: u64, // 成功复制到的日志索引；失败时为 0。Replicated index on success; 0 on failure.
}
