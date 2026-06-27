use serde::{Deserialize, Serialize};

// InstallSnapshot RPC 参数：Leader 在 Follower 落后于本地快照边界时发送完整快照。
// InstallSnapshot RPC args: sent when a follower is behind the leader's compacted prefix.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InstallSnapshotArgs {
    pub(crate) term: u64,
    pub(crate) leader_id: u64,
    pub(crate) last_included_index: u64,
    pub(crate) last_included_term: u64,
    pub(crate) data: Vec<u8>,
}

// InstallSnapshot RPC 响应：Follower 告知当前任期，Leader 据此决定是否退位。
// InstallSnapshot RPC reply: follower reports its current term to the leader.
#[derive(Debug, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub(crate) node_id: u64,
    pub(crate) term: u64,
}
