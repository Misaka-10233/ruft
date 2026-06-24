use crate::utilis::types::LogEntry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppendEntriesArgs {
    pub(crate) term: u64,           // Leader 任期
    pub(crate) leader_id: u64,      // Leader ID
    pub(crate) prev_log_index: u64, // 前一个日志的索引

    pub(crate) prev_log_term: u64,     // 前一个日志的任期
    pub(crate) entries: Vec<LogEntry>, // 新的日志条目

    pub(crate) leader_commit: u64, // Leader 提交的索引
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub(crate) node_id: u64,  // Node ID
    pub(crate) term: u64,     // Leader 任期
    pub(crate) success: bool, // 是否成功
}
