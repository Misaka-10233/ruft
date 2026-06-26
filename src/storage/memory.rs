use crate::storage::{
    HardState, Storage, StorageError, StorageResult, StorageState, sentinel_log_entry,
    validate_sentinel_log,
};
use crate::utilis::types::LogEntry;

// 内存存储实现：主要用于测试和默认无持久化场景。
// In-memory storage backend: mainly for tests and default non-durable use.
#[derive(Clone, Debug)]
pub struct MemoryStorage {
    // 当前持久状态的内存副本。
    // In-memory copy of the persistent state.
    state: StorageState,
}

impl MemoryStorage {
    // 创建空存储，包含默认 hard state 和 log[0] 哨兵项。
    // Creates empty storage with default hard state and the log[0] sentinel.
    pub fn new() -> Self {
        Self {
            state: StorageState {
                hard_state: HardState::default(),
                log: vec![sentinel_log_entry()],
            },
        }
    }

    // 用指定状态创建内存存储；恢复路径测试可用它注入已有日志。
    // Creates storage from a supplied state; useful for recovery-path tests.
    pub fn with_state(state: StorageState) -> StorageResult<Self> {
        validate_sentinel_log(&state.log)?;
        Ok(Self { state })
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    // 返回状态副本，避免调用方绕过 Storage trait 修改内部状态。
    // Returns a copy so callers cannot mutate internal state outside the trait.
    fn load(&self) -> StorageResult<StorageState> {
        Ok(self.state.clone())
    }

    // 覆盖 hard state；内存实现不需要额外刷盘。
    // Replaces hard state; memory storage needs no extra flush.
    fn save_hard_state(&mut self, hard_state: HardState) -> StorageResult<()> {
        self.state.hard_state = hard_state;
        Ok(())
    }

    // 顺序追加日志；空切片自然是 no-op。
    // Appends entries in order; an empty slice is naturally a no-op.
    fn append_entries(&mut self, entries: &[LogEntry]) -> StorageResult<()> {
        self.state.log.extend_from_slice(entries);
        Ok(())
    }

    // 截断日志后缀，但永远保留 log[0] 哨兵项。
    // Truncates the log suffix while always preserving the log[0] sentinel.
    fn truncate_suffix(&mut self, first_index_to_remove: u64) -> StorageResult<()> {
        let len = self.state.log.len() as u64;
        if first_index_to_remove == 0 {
            return Err(StorageError::InvalidOperation(
                "cannot truncate the sentinel log entry".to_string(),
            ));
        }
        if first_index_to_remove > len {
            return Err(StorageError::InvalidOperation(format!(
                "truncate index {first_index_to_remove} is past log length {len}"
            )));
        }

        self.state.log.truncate(first_index_to_remove as usize);
        Ok(())
    }

    // 先校验边界，再一次性替换后缀；内存实现不会发生中途 IO 失败。
    // Validates bounds before replacing the suffix; memory cannot fail midway on IO.
    fn replace_suffix(
        &mut self,
        first_index_to_remove: u64,
        entries: &[LogEntry],
    ) -> StorageResult<()> {
        let len = self.state.log.len() as u64;
        if first_index_to_remove == 0 {
            return Err(StorageError::InvalidOperation(
                "cannot replace the sentinel log entry".to_string(),
            ));
        }
        if first_index_to_remove > len {
            return Err(StorageError::InvalidOperation(format!(
                "replace index {first_index_to_remove} is past log length {len}"
            )));
        }

        self.state.log.truncate(first_index_to_remove as usize);
        self.state.log.extend_from_slice(entries);
        Ok(())
    }

    // 内存存储没有刷盘动作，保留接口以匹配持久化后端语义。
    // Memory storage has nothing to flush; this preserves durable-backend semantics.
    fn sync(&mut self) -> StorageResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: u64, command: &[u8]) -> LogEntry {
        LogEntry {
            term,
            command: command.to_vec(),
        }
    }

    #[test]
    fn new_loads_default_hard_state_and_sentinel_log() {
        let storage = MemoryStorage::new();

        let state = storage.load().expect("load memory storage");

        assert_eq!(state.hard_state, HardState::default());
        assert_eq!(state.log, vec![sentinel_log_entry()]);
    }

    #[test]
    fn save_hard_state_updates_loaded_state() {
        let mut storage = MemoryStorage::new();
        let hard_state = HardState {
            current_term: 3,
            voted_for: Some(2),
        };

        storage
            .save_hard_state(hard_state.clone())
            .expect("save hard state");

        assert_eq!(storage.load().expect("load").hard_state, hard_state);
    }

    #[test]
    fn append_entries_keeps_order() {
        let mut storage = MemoryStorage::new();
        let entries = vec![entry(1, b"one"), entry(2, b"two")];

        storage.append_entries(&entries).expect("append entries");

        let state = storage.load().expect("load");
        assert_eq!(
            state.log,
            vec![sentinel_log_entry(), entry(1, b"one"), entry(2, b"two")]
        );
    }

    #[test]
    fn append_empty_entries_is_noop() {
        let mut storage = MemoryStorage::new();

        storage.append_entries(&[]).expect("append empty entries");

        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry()]
        );
    }

    #[test]
    fn truncate_suffix_keeps_sentinel() {
        let mut storage = MemoryStorage::new();
        storage
            .append_entries(&[entry(1, b"one"), entry(1, b"two")])
            .expect("append entries");

        storage.truncate_suffix(1).expect("truncate suffix");

        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry()]
        );
    }

    #[test]
    fn truncate_suffix_zero_is_invalid() {
        let mut storage = MemoryStorage::new();

        let err = storage
            .truncate_suffix(0)
            .expect_err("truncate should fail");

        assert!(matches!(err, StorageError::InvalidOperation(_)));
    }

    #[test]
    fn truncate_suffix_at_log_len_is_noop() {
        let mut storage = MemoryStorage::new();
        storage.append_entries(&[entry(1, b"one")]).expect("append");

        storage.truncate_suffix(2).expect("truncate at len");

        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry(), entry(1, b"one")]
        );
    }

    #[test]
    fn truncate_suffix_past_log_len_is_invalid() {
        let mut storage = MemoryStorage::new();

        let err = storage
            .truncate_suffix(2)
            .expect_err("truncate should fail");

        assert!(matches!(err, StorageError::InvalidOperation(_)));
    }

    #[test]
    fn replace_suffix_replaces_existing_tail() {
        let mut storage = MemoryStorage::new();
        storage
            .append_entries(&[entry(1, b"one"), entry(1, b"old")])
            .expect("append entries");

        storage
            .replace_suffix(2, &[entry(2, b"new")])
            .expect("replace suffix");

        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry(), entry(1, b"one"), entry(2, b"new")]
        );
    }

    #[test]
    fn replace_suffix_at_log_len_appends() {
        let mut storage = MemoryStorage::new();
        storage.append_entries(&[entry(1, b"one")]).expect("append");

        storage
            .replace_suffix(2, &[entry(2, b"two")])
            .expect("replace at len");

        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry(), entry(1, b"one"), entry(2, b"two")]
        );
    }

    #[test]
    fn replace_suffix_zero_is_invalid() {
        let mut storage = MemoryStorage::new();

        let err = storage
            .replace_suffix(0, &[entry(1, b"one")])
            .expect_err("replace should fail");

        assert!(matches!(err, StorageError::InvalidOperation(_)));
    }

    #[test]
    fn replace_suffix_past_log_len_is_invalid() {
        let mut storage = MemoryStorage::new();

        let err = storage
            .replace_suffix(2, &[entry(1, b"one")])
            .expect_err("replace should fail");

        assert!(matches!(err, StorageError::InvalidOperation(_)));
    }

    #[test]
    fn sync_is_noop() {
        let mut storage = MemoryStorage::new();

        storage.sync().expect("sync");

        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry()]
        );
    }

    #[test]
    fn with_state_rejects_missing_sentinel() {
        let state = StorageState {
            hard_state: HardState::default(),
            log: vec![entry(1, b"not sentinel")],
        };

        let err = MemoryStorage::with_state(state).expect_err("state should fail validation");

        assert!(matches!(err, StorageError::InvalidOperation(_)));
    }
}
