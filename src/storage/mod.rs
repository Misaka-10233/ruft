mod file;
mod memory;
mod utils;

use crate::rpc::client::NodeId;
use crate::ruft::{LogEntry, Snapshot};
use std::error::Error;
use std::fmt::{Display, Formatter};

pub use file::FileStorage;
pub use memory::MemoryStorage;
use serde::{Deserialize, Serialize};
pub use utils::{checksum, node_storage_dir};

// 存储层统一结果类型，便于后续文件 WAL 和快照实现复用。
pub type StorageResult<T> = Result<T, StorageError>;

// 存储层错误：区分底层 IO、调用方非法操作和已落盘数据损坏。
#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),       // 底层读写失败。
    InvalidOperation(String), // 调用方违反日志边界等约束。
    Corruption(String),       // 持久化数据损坏。
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(err) => write!(f, "storage IO error: {err}"),
            StorageError::InvalidOperation(message) => {
                write!(f, "invalid storage operation: {message}")
            }
            StorageError::Corruption(message) => write!(f, "storage corruption: {message}"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StorageError::Io(err) => Some(err),
            StorageError::InvalidOperation(_) => None,
            StorageError::Corruption(_) => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        StorageError::Io(value)
    }
}

// Raft 必须持久化的任期和投票状态。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: u64,         // 当前任期。
    pub voted_for: Option<NodeId>, // 当前任期投给的节点。
}

impl Default for HardState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}

// 从存储层恢复出的完整 Raft 持久状态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageState {
    pub hard_state: HardState, // 持久化任期和投票状态。
    pub log: Vec<LogEntry>,    // 持久化日志；log[0] 是空哨兵。
    pub snapshot: Option<Snapshot>,
}

// Raft 存储抽象：状态机只依赖这些语义，不关心底层是内存、WAL 还是快照。
pub trait Storage: Send + Sync {
    // 读取当前持久状态；启动恢复时会使用。
    fn load(&self) -> StorageResult<StorageState>;

    // 保存任期和投票状态；在回复 RPC 或发起选举前必须持久化。
    fn save_hard_state(&mut self, hard_state: HardState) -> StorageResult<()>;

    // 追加日志条目；生产后端应保证成功返回前数据已进入持久化路径。
    fn append_entries(&mut self, entries: &[LogEntry]) -> StorageResult<()>;

    // 删除从指定索引开始的后缀日志，用于处理 Leader 覆盖冲突日志。
    fn truncate_suffix(&mut self, first_index_to_remove: u64) -> StorageResult<()>;

    // 原子替换从指定索引开始的日志后缀，避免 truncate/append 分步失败造成半更新。
    fn replace_suffix(
        &mut self,
        first_index_to_remove: u64,
        entries: &[LogEntry],
    ) -> StorageResult<()>;

    // 刷盘边界；内存实现为空操作，文件实现应在这里 fsync。
    fn sync(&mut self) -> StorageResult<()>;

    // 保存状态机快照及其元数据。
    fn save_snapshot(&mut self, snapshot: Snapshot) -> StorageResult<()>;

    // 丢弃已包含在快照中的日志前缀。
    fn compact_log(
        &mut self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> StorageResult<()>;
}

// 创建当前实现统一使用的空哨兵日志，保持 Raft 日志索引从 1 开始。
pub(crate) fn sentinel_log_entry() -> LogEntry {
    LogEntry {
        term: 0,
        command: Vec::new(),
    }
}

// 校验恢复出的日志是否保留哨兵项，防止后续索引计算越界或错位。
pub(crate) fn validate_sentinel_log(log: &[LogEntry]) -> StorageResult<()> {
    if log.first().is_some_and(|entry| entry.command.is_empty()) {
        return Ok(());
    }

    Err(StorageError::InvalidOperation(
        "log must start with an empty sentinel entry".to_string(),
    ))
}
