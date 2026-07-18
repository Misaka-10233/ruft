// Raft 核心模块：角色定义和节点状态机实现。
mod role;
mod ruft;

pub use role::Role;
pub use ruft::{ApplyMsg, LogEntry, Ruft, RuftHandle, RuftInfo, Snapshot, SnapshotMetadata};
