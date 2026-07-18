use serde::{Deserialize, Serialize};

// InstallSnapshot RPC 参数：Leader 在 Follower 落后于本地快照边界时发送完整快照。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InstallSnapshotArgs {
    pub(crate) term: u64,                // Leader 任期。
    pub(crate) leader_id: u64,           // Leader ID。
    pub(crate) last_included_index: u64, // 快照包含的最后一条日志索引。
    pub(crate) last_included_term: u64,  // 最后一条日志的任期。
    pub(crate) data: Vec<u8>,            // 快照数据。
}

// InstallSnapshot RPC 响应：Follower 告知当前任期，Leader 据此决定是否退位。
#[derive(Debug, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub(crate) node_id: u64, // 响应节点 ID。
    pub(crate) term: u64,    // 响应节点当前任期。
}
