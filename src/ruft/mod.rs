// Raft 核心模块：角色定义和节点状态机实现。
// Raft core module: role definitions and node state machine implementation.
mod role;
mod ruft;

pub use role::Role;
pub use ruft::{ApplyMsg, LogEntry, Ruft, RuftHandle, RuftInfo, Snapshot, SnapshotMetadata};
