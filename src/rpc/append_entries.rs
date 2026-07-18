use crate::ruft::LogEntry;
use serde::{Deserialize, Serialize};

// AppendEntries RPC 参数：Leader 用它复制日志，也用空 entries 发送心跳。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppendEntriesArgs {
    pub(crate) term: u64,           // Leader 任期。
    pub(crate) leader_id: u64,      // Leader ID。
    pub(crate) prev_log_index: u64, // 新条目前一条日志索引。

    pub(crate) prev_log_term: u64,     // 新条目前一条日志任期。
    pub(crate) entries: Vec<LogEntry>, // 待复制的新日志条目。

    pub(crate) leader_commit: u64, // Leader 已提交索引。
}

// AppendEntries RPC 响应：Follower 告诉 Leader 当前任期和复制是否成功。
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub(crate) node_id: u64,     // 响应节点 ID。
    pub(crate) term: u64,        // 响应节点当前任期。
    pub(crate) success: bool,    // 日志匹配并写入成功。
    pub(crate) match_index: u64, // 成功复制到的日志索引；失败时为 0。
}
