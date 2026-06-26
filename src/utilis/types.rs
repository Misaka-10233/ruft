use serde::{Deserialize, Serialize};

// Raft 日志条目：记录产生该命令的任期和要应用到状态机的命令字节。
// Raft log entry: stores the term and command bytes applied to the state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,        // 写入该日志的任期。Term when this entry was written.
    pub command: Vec<u8>, // 状态机命令。State-machine command.
}
