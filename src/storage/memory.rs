use crate::ruft::{LogEntry, Snapshot};
use crate::storage::{
    HardState, Storage, StorageError, StorageResult, StorageState, sentinel_log_entry,
    validate_sentinel_log,
};

// 内存存储实现：主要用于测试和默认无持久化场景。
#[derive(Clone, Debug)]
pub struct MemoryStorage {
    // 当前持久状态的内存副本。
    state: StorageState,
    compact_base_index: u64,
}

impl MemoryStorage {
    // 创建空存储，包含默认 hard state 和 log[0] 哨兵项。
    pub fn new() -> Self {
        Self {
            state: StorageState {
                hard_state: HardState::default(),
                log: vec![sentinel_log_entry()],
                snapshot: None,
            },
            compact_base_index: 0,
        }
    }

    // 用指定状态创建内存存储；恢复路径测试可用它注入已有日志。
    pub fn with_state(state: StorageState) -> StorageResult<Self> {
        validate_sentinel_log(&state.log)?;
        let compact_base_index = state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.metadata.last_included_index)
            .unwrap_or(0);
        Ok(Self {
            state,
            compact_base_index,
        })
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    // 返回状态副本，避免调用方绕过 Storage trait 修改内部状态。
    fn load(&self) -> StorageResult<StorageState> {
        Ok(self.state.clone())
    }

    // 覆盖 hard state；内存实现不需要额外刷盘。
    fn save_hard_state(&mut self, hard_state: HardState) -> StorageResult<()> {
        self.state.hard_state = hard_state;
        Ok(())
    }

    // 顺序追加日志；空切片自然是 no-op。
    fn append_entries(&mut self, entries: &[LogEntry]) -> StorageResult<()> {
        self.state.log.extend_from_slice(entries);
        Ok(())
    }

    // 截断日志后缀，但永远保留 log[0] 哨兵项。
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
    fn sync(&mut self) -> StorageResult<()> {
        Ok(())
    }

    fn save_snapshot(&mut self, snapshot: Snapshot) -> StorageResult<()> {
        self.state.snapshot = Some(snapshot);
        Ok(())
    }

    fn compact_log(
        &mut self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> StorageResult<()> {
        if last_included_index < self.compact_base_index {
            return Err(StorageError::InvalidOperation(format!(
                "compact index {last_included_index} is before base index {}",
                self.compact_base_index
            )));
        }
        let local_index = last_included_index - self.compact_base_index;
        compact_log_state(&mut self.state.log, local_index, last_included_term)?;
        self.compact_base_index = last_included_index;
        Ok(())
    }
}

pub(crate) fn compact_log_state(
    log: &mut Vec<LogEntry>,
    local_included_index: u64,
    last_included_term: u64,
) -> StorageResult<()> {
    // 将快照边界改为新的哨兵项，并保留尚未包含在快照中的后缀。
    let len = log.len() as u64;
    let mut compacted = vec![LogEntry {
        term: last_included_term,
        command: Vec::new(),
    }];
    if local_included_index + 1 < len {
        compacted.extend_from_slice(&log[(local_included_index + 1) as usize..]);
    }
    *log = compacted;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ruft::SnapshotMetadata;

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
    fn snapshot_and_compaction_update_loaded_state() {
        let mut storage = MemoryStorage::new();
        storage
            .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
            .expect("append");
        let snapshot = Snapshot {
            metadata: SnapshotMetadata {
                last_included_index: 2,
                last_included_term: 2,
            },
            data: b"state".to_vec(),
        };

        storage
            .save_snapshot(snapshot.clone())
            .expect("save snapshot");
        storage.compact_log(2, 2).expect("compact");

        let state = storage.load().expect("load");
        assert_eq!(state.snapshot, Some(snapshot));
        assert_eq!(state.log, vec![entry(2, b""), entry(3, b"three")]);
    }

    #[test]
    fn with_state_rejects_missing_sentinel() {
        let state = StorageState {
            hard_state: HardState::default(),
            log: vec![entry(1, b"not sentinel")],
            snapshot: None,
        };

        let err = MemoryStorage::with_state(state).expect_err("state should fail validation");

        assert!(matches!(err, StorageError::InvalidOperation(_)));
    }
}
