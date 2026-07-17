use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::rpc::client::NodeId;
use crate::ruft::{LogEntry, Snapshot};
use crate::storage::memory::compact_log_state;
use crate::storage::{
    HardState, Storage, StorageError, StorageResult, StorageState, checksum, sentinel_log_entry,
    utils, validate_sentinel_log,
};

// WAL 文件头包含 magic 和版本号；后续扩展格式时应新版本化而不是静默兼容。
// WAL header contains magic and version; future format changes should version this explicitly.
const WAL_HEADER: &[u8; 7] = b"RUFTWAL";
const WAL_FILE_NAME: &str = "node.wal";
const HARD_STATE_FILE_NAME: &str = "hard_state";
const HARD_STATE_TMP_FILE_NAME: &str = "hard_state.tmp";
const SNAPSHOT_FILE_PATH: &str = "snapshot";
const SNAPSHOT_TMP_FILE_PATH: &str = "snapshot.tmp";

// WAL 只记录会改变日志镜像的操作；恢复时按顺序回放即可得到完整 log。
// WAL records only log-mutating operations; replaying them in order reconstructs the log.
#[derive(Debug, Serialize, Deserialize)]
enum WalRecord {
    // 顺序追加一批日志条目。
    // Appends a batch of log entries in order.
    Append(Vec<LogEntry>),
    // 原子替换后缀：先截断到指定索引，再追加新的后缀条目。
    // Atomically replaces the suffix by truncating at the index, then appending entries.
    ReplaceSuffix {
        first_index_to_remove: u64,
        entries: Vec<LogEntry>,
    },
    // 声明后续记录相对于哪个绝对日志索引；压缩重写 WAL 时首先写入。
    // Declares the absolute log index subsequent records are relative to.
    SetBase {
        index: u64,
        term: u64,
    },
}

// 文件存储实现：hard_state 使用原子替换，log 使用单段 WAL 记录追加。
// File storage backend: hard_state is atomically replaced, log uses a single WAL segment.
#[derive(Debug)]
pub struct FileStorage {
    // 节点持久化目录；hard_state 和 wal/ 都位于此目录下。
    // Persistent directory for this node; hard_state and wal/ live under it.
    root_dir: PathBuf,
    // hard_state 文件路径，保存 current_term 和 voted_for。
    // Path to the hard_state file storing current_term and voted_for.
    hard_state_path: PathBuf,
    // 当前单段 WAL 文件句柄；追加记录和 sync 都复用它。
    // Handle to the current single-segment WAL file, reused for appends and sync.
    wal_file: File,

    snapshot_path: PathBuf,
    compact_base_index: u64,
    // 从磁盘恢复后维护的内存镜像，避免每次 load 都重新回放 WAL。
    // In-memory mirror restored from disk, avoiding WAL replay on every load.
    state: StorageState,
}

impl FileStorage {
    // 打开或创建持久化存储，并从 hard_state + WAL 恢复完整持久状态。
    // Opens or creates persistent storage, then restores state from hard_state + WAL.
    pub fn open(root: impl AsRef<Path>, node_id: NodeId) -> StorageResult<Self> {
        let root_dir = utils::node_storage_dir(root.as_ref(), node_id);
        let wal_dir = root_dir.join("wal");
        let hard_state_path = root_dir.join(HARD_STATE_FILE_NAME);
        let snapshot_path = root_dir.join(SNAPSHOT_FILE_PATH);
        let wal_path = wal_dir.join(WAL_FILE_NAME);

        fs::create_dir_all(&wal_dir)?;

        let hard_state = load_hard_state(&hard_state_path)?;
        let snapshot = load_snapshot(&snapshot_path)?;
        let mut wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;

        let (mut log, wal_base_index) = load_wal(&mut wal_file)?;
        let (compact_base_index, rewrite_wal) = match (&snapshot, wal_base_index) {
            (Some(snapshot), Some(wal_base_index)) => {
                let snapshot_index = snapshot.metadata.last_included_index;
                if wal_base_index > snapshot_index {
                    return Err(StorageError::Corruption(format!(
                        "WAL base index {wal_base_index} is after snapshot index {snapshot_index}"
                    )));
                }
                let covered_entries = snapshot_index - wal_base_index;
                if covered_entries >= log.len() as u64 {
                    log = vec![LogEntry {
                        term: snapshot.metadata.last_included_term,
                        command: Vec::new(),
                    }];
                } else {
                    log.drain(0..covered_entries as usize);
                    log[0] = LogEntry {
                        term: snapshot.metadata.last_included_term,
                        command: Vec::new(),
                    };
                }
                (snapshot_index, wal_base_index != snapshot_index)
            }
            (Some(snapshot), None) => {
                let snapshot_index = snapshot.metadata.last_included_index;
                log[0].term = snapshot.metadata.last_included_term;
                (snapshot_index, true)
            }
            (None, Some(wal_base_index)) => {
                if wal_base_index != 0 {
                    return Err(StorageError::Corruption(format!(
                        "WAL base index {wal_base_index} requires a snapshot"
                    )));
                }
                (0, false)
            }
            (None, None) => (0, true),
        };
        let state = StorageState {
            hard_state,
            log,
            snapshot,
        };
        validate_sentinel_log(&state.log)?;

        wal_file.seek(SeekFrom::End(0))?;

        let mut storage = Self {
            root_dir,
            hard_state_path,
            wal_file,
            snapshot_path,
            compact_base_index,
            state,
        };
        if rewrite_wal {
            storage.rewrite_wal()?;
        }
        Ok(storage)
    }

    // 将一条 WAL 记录编码为 len + crc + payload 并追加到文件尾。
    // Encodes one WAL record as len + crc + payload and appends it to the file tail.
    fn append_record(&mut self, record: &WalRecord) -> StorageResult<()> {
        let payload = bincode::serialize(record).map_err(|err| {
            StorageError::InvalidOperation(format!("serialize WAL record: {err}"))
        })?;
        let len = u32::try_from(payload.len()).map_err(|_| {
            StorageError::InvalidOperation("WAL record payload is larger than u32::MAX".to_string())
        })?;
        let crc = checksum(&payload);

        self.wal_file.seek(SeekFrom::End(0))?;
        self.wal_file.write_all(&len.to_le_bytes())?;
        self.wal_file.write_all(&crc.to_le_bytes())?;
        self.wal_file.write_all(&payload)?;
        Ok(())
    }

    fn rewrite_wal(&mut self) -> StorageResult<()> {
        self.wal_file.set_len(0)?;
        self.wal_file.seek(SeekFrom::Start(0))?;
        self.wal_file.write_all(WAL_HEADER)?;
        self.append_record(&WalRecord::SetBase {
            index: self.compact_base_index,
            term: self.state.log[0].term,
        })?;
        let tail = if self.state.log.len() > 1 {
            self.state.log[1..].to_vec()
        } else {
            Vec::new()
        };
        if !tail.is_empty() {
            self.append_record(&WalRecord::Append(tail))?;
        }
        self.wal_file.flush()?;
        self.wal_file.sync_data()?;
        Ok(())
    }

    // 校验后缀替换边界，永远不允许删除 log[0] 哨兵项。
    // Validates suffix replacement bounds and never allows deleting the log[0] sentinel.
    fn validate_suffix_index(
        &self,
        operation: &str,
        first_index_to_remove: u64,
    ) -> StorageResult<()> {
        let len = self.state.log.len() as u64;
        if first_index_to_remove == 0 {
            return Err(StorageError::InvalidOperation(format!(
                "cannot {operation} the sentinel log entry"
            )));
        }
        if first_index_to_remove > len {
            return Err(StorageError::InvalidOperation(format!(
                "{operation} index {first_index_to_remove} is past log length {len}"
            )));
        }
        Ok(())
    }
}

impl Storage for FileStorage {
    // 返回已恢复并随写入同步维护的状态副本。
    // Returns a copy of the restored state mirror kept up to date with writes.
    fn load(&self) -> StorageResult<StorageState> {
        Ok(self.state.clone())
    }

    // 通过 tmp + fsync + rename 原子保存 hard state，避免崩溃留下半写文件。
    // Saves hard state atomically with tmp + fsync + rename to avoid half-written files.
    fn save_hard_state(&mut self, hard_state: HardState) -> StorageResult<()> {
        let tmp_path = self.root_dir.join(HARD_STATE_TMP_FILE_NAME);
        let encoded = bincode::serialize(&hard_state).map_err(|err| {
            StorageError::InvalidOperation(format!("serialize hard state: {err}"))
        })?;

        {
            let mut tmp = File::create(&tmp_path)?;
            tmp.write_all(&encoded)?;
            tmp.flush()?;
            tmp.sync_all()?;
        }

        fs::rename(&tmp_path, &self.hard_state_path)?;
        // Windows 对目录 fsync 支持有限；这里 best-effort 固化 rename 元数据。
        // Directory fsync support is limited on Windows; this best-effort call hardens rename metadata.
        sync_dir_best_effort(&self.root_dir);
        self.state.hard_state = hard_state;
        Ok(())
    }

    // 先写 WAL 记录，再更新内存镜像；调用方随后通过 sync() 定义刷盘边界。
    // Writes the WAL record before updating the memory mirror; callers define durability via sync().
    fn append_entries(&mut self, entries: &[LogEntry]) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        self.append_record(&WalRecord::Append(entries.to_vec()))?;
        self.state.log.extend_from_slice(entries);
        Ok(())
    }

    // 截断是空后缀替换的特例，共用同一条 WAL 记录格式。
    // Truncation is a suffix replacement with no new entries, sharing the same WAL record format.
    fn truncate_suffix(&mut self, first_index_to_remove: u64) -> StorageResult<()> {
        self.replace_suffix(first_index_to_remove, &[])
    }

    // 使用单条 ReplaceSuffix 记录表达截断+追加，避免恢复时看到半个逻辑更新。
    // Uses one ReplaceSuffix record for truncate+append so recovery never observes a half update.
    fn replace_suffix(
        &mut self,
        first_index_to_remove: u64,
        entries: &[LogEntry],
    ) -> StorageResult<()> {
        self.validate_suffix_index("replace", first_index_to_remove)?;

        self.append_record(&WalRecord::ReplaceSuffix {
            first_index_to_remove,
            entries: entries.to_vec(),
        })?;
        self.state.log.truncate(first_index_to_remove as usize);
        self.state.log.extend_from_slice(entries);
        Ok(())
    }

    // 刷新 WAL 文件内容；hard_state 在 save_hard_state 内部已经单独 fsync。
    // Flushes WAL contents; hard_state is fsynced inside save_hard_state separately.
    fn sync(&mut self) -> StorageResult<()> {
        self.wal_file.flush()?;
        self.wal_file.sync_data()?;
        Ok(())
    }

    fn save_snapshot(&mut self, snapshot: crate::ruft::Snapshot) -> StorageResult<()> {
        let temp_path = self.root_dir.join(SNAPSHOT_TMP_FILE_PATH);
        let encoded = bincode::serialize(&snapshot)
            .map_err(|err| StorageError::InvalidOperation(format!("serialize snapshot: {err}")))?;
        {
            let mut tmp = File::create(&temp_path)?;
            tmp.write_all(&encoded)?;
            tmp.flush()?;
            tmp.sync_all()?;
        }

        fs::rename(temp_path, &self.snapshot_path)?;
        sync_dir_best_effort(&self.root_dir);
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
        self.rewrite_wal()
    }
}

// 读取 hard_state；缺失表示首次启动，损坏则拒绝启动。
// Loads hard_state; absence means first boot, corruption refuses startup.
fn load_hard_state(path: &Path) -> StorageResult<HardState> {
    match fs::read(path) {
        Ok(bytes) => bincode::deserialize(&bytes)
            .map_err(|err| StorageError::Corruption(format!("invalid hard_state file: {err}"))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HardState::default()),
        Err(err) => Err(StorageError::Io(err)),
    }
}

fn load_snapshot(path: &Path) -> StorageResult<Option<Snapshot>> {
    match fs::read(path) {
        Ok(bytes) => bincode::deserialize(&bytes)
            .map(Some)
            .map_err(|err| StorageError::Corruption(format!("invalid snapshot file: {err}"))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StorageError::Io(err)),
    }
}

// 回放 WAL 得到日志镜像及其绝对基线；仅允许截断文件尾部的半写记录。
// Replays WAL into a log mirror; only half-written tail records may be truncated.
fn load_wal(file: &mut File) -> StorageResult<(Vec<LogEntry>, Option<u64>)> {
    let mut log = vec![sentinel_log_entry()];
    let mut base_index = None;
    let file_len = file.metadata()?.len();

    if file_len == 0 {
        // 新 WAL 初始化文件头并立即刷盘，保证下次启动能识别格式。
        // A new WAL writes and syncs the header so the next startup can identify the format.
        file.write_all(WAL_HEADER)?;
        file.flush()?;
        file.sync_data()?;
        return Ok((log, base_index));
    }

    if file_len < WAL_HEADER.len() as u64 {
        // 只有文件尾部损坏到不足 header 时才重建；此时没有完整 record 可保留。
        // Rebuild only when the file is shorter than the header; no complete record can exist.
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(WAL_HEADER)?;
        file.flush()?;
        file.sync_data()?;
        return Ok((log, base_index));
    }

    file.seek(SeekFrom::Start(0))?;
    let mut header = [0_u8; WAL_HEADER.len()];
    file.read_exact(&mut header)?;
    if &header != WAL_HEADER {
        return Err(StorageError::Corruption(
            "invalid WAL header magic/version".to_string(),
        ));
    }

    let mut valid_end = WAL_HEADER.len() as u64;
    loop {
        let record_start = valid_end;
        file.seek(SeekFrom::Start(record_start))?;

        let mut record_header = [0_u8; 8];
        let header_bytes = read_some(file, &mut record_header)?;
        if header_bytes == 0 {
            break;
        }
        if header_bytes < record_header.len() {
            // record 头部半写说明崩溃发生在文件尾部，截断到上一个有效位置。
            // A partial record header means a tail crash write; truncate to the previous valid end.
            file.set_len(valid_end)?;
            break;
        }

        let payload_len = u32::from_le_bytes(record_header[0..4].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(record_header[4..8].try_into().unwrap());
        let payload_start = record_start + record_header.len() as u64;
        let payload_end = payload_start + payload_len as u64;
        if payload_end > file_len {
            // payload 半写同样只允许出现在尾部，截断后保留所有完整记录。
            // A partial payload is also allowed only at the tail, preserving all complete records.
            file.set_len(valid_end)?;
            break;
        }

        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)?;

        let actual_crc = checksum(&payload);
        if actual_crc != expected_crc {
            return Err(StorageError::Corruption(format!(
                "WAL record CRC mismatch at offset {record_start}"
            )));
        }

        let record: WalRecord = bincode::deserialize(&payload).map_err(|err| {
            StorageError::Corruption(format!(
                "invalid WAL record payload at offset {record_start}: {err}"
            ))
        })?;
        apply_record(&mut log, &mut base_index, record, record_start)?;
        valid_end = payload_end;
    }

    file.seek(SeekFrom::Start(valid_end))?;
    Ok((log, base_index))
}

// 尝试读满缓冲区；遇到 EOF 返回已读字节数，用于识别尾部半写。
// Tries to fill the buffer; EOF returns bytes read so tail partial writes can be detected.
fn read_some(file: &mut File, buf: &mut [u8]) -> StorageResult<usize> {
    let mut read = 0;
    while read < buf.len() {
        match file.read(&mut buf[read..]) {
            Ok(0) => return Ok(read),
            Ok(n) => read += n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => return Err(StorageError::Io(err)),
        }
    }
    Ok(read)
}

// 将单条 WAL 记录应用到恢复中的日志，并校验记录不会破坏哨兵边界。
// Applies one WAL record to the recovering log and validates sentinel-safe boundaries.
fn apply_record(
    log: &mut Vec<LogEntry>,
    base_index: &mut Option<u64>,
    record: WalRecord,
    offset: u64,
) -> StorageResult<()> {
    match record {
        WalRecord::SetBase { index, term } => {
            if log.len() != 1 || base_index.is_some() {
                return Err(StorageError::Corruption(format!(
                    "WAL base record is not the first record at offset {offset}"
                )));
            }
            *base_index = Some(index);
            log[0].term = term;
            Ok(())
        }
        WalRecord::Append(entries) => {
            log.extend(entries);
            Ok(())
        }
        WalRecord::ReplaceSuffix {
            first_index_to_remove,
            entries,
        } => {
            let len = log.len() as u64;
            if first_index_to_remove == 0 || first_index_to_remove > len {
                return Err(StorageError::Corruption(format!(
                    "invalid ReplaceSuffix index {first_index_to_remove} for log length {len} at offset {offset}"
                )));
            }
            log.truncate(first_index_to_remove as usize);
            log.extend(entries);
            Ok(())
        }
    }
}

// 尽力同步目录元数据；不支持目录 fsync 的平台上静默退化。
// Best-effort directory metadata sync; silently degrades on platforms without directory fsync.
fn sync_dir_best_effort(path: &Path) {
    if let Ok(dir) = File::open(path) {
        let _ = dir.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::utils::node_id_hash;
    use crate::storage::{Storage, node_storage_dir};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> std::io::Result<Self> {
            let mut path = std::env::temp_dir();
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let seq = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            path.push(format!(
                "ruft-file-storage-{}-{nanos}-{seq}",
                std::process::id()
            ));
            fs::create_dir_all(&path)?;
            Ok(Self { path })
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn entry(term: u64, command: &[u8]) -> LogEntry {
        LogEntry {
            term,
            command: command.to_vec(),
        }
    }

    const TEST_NODE_ID: NodeId = 42;

    fn open_storage(dir: &TempDir) -> StorageResult<FileStorage> {
        FileStorage::open(dir.path(), TEST_NODE_ID)
    }

    fn wal_path(dir: &TempDir) -> PathBuf {
        node_storage_dir(dir.path(), TEST_NODE_ID)
            .join("wal")
            .join(WAL_FILE_NAME)
    }

    fn hard_state_path(dir: &TempDir) -> PathBuf {
        node_storage_dir(dir.path(), TEST_NODE_ID).join(HARD_STATE_FILE_NAME)
    }

    fn snapshot(index: u64, term: u64, data: &[u8]) -> Snapshot {
        Snapshot {
            metadata: crate::ruft::SnapshotMetadata {
                last_included_index: index,
                last_included_term: term,
            },
            data: data.to_vec(),
        }
    }

    #[test]
    fn open_empty_directory_loads_default_state() {
        let dir = TempDir::new().expect("temp dir");
        let storage = open_storage(&dir).expect("open file storage");

        let state = storage.load().expect("load");
        assert_eq!(state.hard_state, HardState::default());
        assert_eq!(state.log, vec![sentinel_log_entry()]);
    }

    #[test]
    fn open_uses_hashed_node_id_directory() {
        let dir = TempDir::new().expect("temp dir");
        let node_id = 7;
        let storage = FileStorage::open(dir.path(), node_id).expect("open file storage");

        assert_eq!(storage.root_dir, node_storage_dir(dir.path(), node_id));
        assert!(storage.root_dir.exists());
        assert!(
            storage
                .root_dir
                .ends_with(format!("node-{:08x}", node_id_hash(node_id)))
        );
        assert!(storage.root_dir.join("wal").join(WAL_FILE_NAME).exists());
        assert!(!dir.path().join("wal").exists());
    }

    #[test]
    fn hard_state_survives_reopen() {
        let dir = TempDir::new().expect("temp dir");
        let hard_state = HardState {
            current_term: 7,
            voted_for: Some(3),
        };

        {
            let mut storage = open_storage(&dir).expect("open file storage");
            storage
                .save_hard_state(hard_state.clone())
                .expect("save hard state");
        }

        assert!(hard_state_path(&dir).exists());

        let storage = open_storage(&dir).expect("reopen file storage");
        assert_eq!(storage.load().expect("load").hard_state, hard_state);
    }

    #[test]
    fn appended_entries_survive_reopen() {
        let dir = TempDir::new().expect("temp dir");
        let entries = vec![entry(1, b"one"), entry(2, b"two")];

        {
            let mut storage = open_storage(&dir).expect("open file storage");
            storage.append_entries(&entries).expect("append");
            storage.sync().expect("sync");
        }

        let storage = open_storage(&dir).expect("reopen file storage");
        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry(), entry(1, b"one"), entry(2, b"two")]
        );
    }

    #[test]
    fn snapshot_and_compacted_log_survive_reopen_without_old_wal_prefix() {
        let dir = TempDir::new().expect("temp dir");
        let snap = snapshot(2, 2, b"state");

        {
            let mut storage = open_storage(&dir).expect("open file storage");
            storage
                .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
                .expect("append");
            storage.save_snapshot(snap.clone()).expect("save snapshot");
            storage.compact_log(2, 2).expect("compact");
            storage.sync().expect("sync");
        }

        let storage = open_storage(&dir).expect("reopen file storage");
        let state = storage.load().expect("load");
        assert_eq!(state.snapshot, Some(snap));
        assert_eq!(state.log, vec![entry(2, b""), entry(3, b"three")]);
    }

    #[test]
    fn reopen_recovers_snapshot_saved_before_wal_compaction() {
        let dir = TempDir::new().expect("temp dir");
        let snap = snapshot(2, 2, b"state");

        {
            let mut storage = open_storage(&dir).expect("open file storage");
            storage
                .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
                .expect("append");
            storage.sync().expect("sync WAL before simulated crash");
            storage.save_snapshot(snap.clone()).expect("save snapshot");
        }

        let storage = open_storage(&dir).expect("reopen after simulated crash");
        let state = storage.load().expect("load");
        assert_eq!(state.snapshot, Some(snap));
        assert_eq!(state.log, vec![entry(2, b""), entry(3, b"three")]);
        drop(storage);

        let storage = open_storage(&dir).expect("reopen repaired WAL");
        assert_eq!(
            storage.load().expect("load").log,
            vec![entry(2, b""), entry(3, b"three")]
        );
    }

    #[test]
    fn reopen_recovers_later_snapshot_saved_before_wal_compaction() {
        let dir = TempDir::new().expect("temp dir");

        {
            let mut storage = open_storage(&dir).expect("open file storage");
            storage
                .append_entries(&[
                    entry(1, b"one"),
                    entry(2, b"two"),
                    entry(3, b"three"),
                    entry(4, b"four"),
                ])
                .expect("append");
            storage
                .save_snapshot(snapshot(2, 2, b"state at two"))
                .expect("save first snapshot");
            storage.compact_log(2, 2).expect("compact first snapshot");
            storage.sync().expect("sync first compaction");
            storage
                .save_snapshot(snapshot(3, 3, b"state at three"))
                .expect("save second snapshot");
        }

        let storage = open_storage(&dir).expect("reopen after second simulated crash");
        let state = storage.load().expect("load");
        assert_eq!(state.snapshot, Some(snapshot(3, 3, b"state at three")));
        assert_eq!(state.log, vec![entry(3, b""), entry(4, b"four")]);
    }

    #[test]
    fn replace_suffix_survives_reopen() {
        let dir = TempDir::new().expect("temp dir");

        {
            let mut storage = open_storage(&dir).expect("open file storage");
            storage
                .append_entries(&[entry(1, b"keep"), entry(1, b"stale")])
                .expect("append");
            storage
                .replace_suffix(2, &[entry(2, b"new")])
                .expect("replace");
            storage.sync().expect("sync");
        }

        let storage = open_storage(&dir).expect("reopen file storage");
        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry(), entry(1, b"keep"), entry(2, b"new")]
        );
    }

    #[test]
    fn truncate_suffix_survives_reopen_and_validates_bounds() {
        let dir = TempDir::new().expect("temp dir");
        let mut storage = open_storage(&dir).expect("open file storage");
        storage
            .append_entries(&[entry(1, b"one"), entry(1, b"two")])
            .expect("append");

        assert!(matches!(
            storage.truncate_suffix(0),
            Err(StorageError::InvalidOperation(_))
        ));
        assert!(matches!(
            storage.truncate_suffix(4),
            Err(StorageError::InvalidOperation(_))
        ));

        storage.truncate_suffix(1).expect("truncate");
        storage.sync().expect("sync");
        drop(storage);

        let storage = open_storage(&dir).expect("reopen file storage");
        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry()]
        );
    }

    #[test]
    fn partial_tail_record_is_truncated_on_reopen() {
        let dir = TempDir::new().expect("temp dir");

        {
            let mut storage = open_storage(&dir).expect("open file storage");
            storage.append_entries(&[entry(1, b"one")]).expect("append");
            storage.sync().expect("sync");
        }

        let mut file = OpenOptions::new()
            .append(true)
            .open(wal_path(&dir))
            .expect("open wal");
        file.write_all(&42_u32.to_le_bytes()).expect("partial len");
        file.write_all(&7_u16.to_le_bytes()).expect("partial crc");
        file.flush().expect("flush");
        drop(file);

        let storage = open_storage(&dir).expect("reopen truncates tail");
        assert_eq!(
            storage.load().expect("load").log,
            vec![sentinel_log_entry(), entry(1, b"one")]
        );

        let len_after = fs::metadata(wal_path(&dir)).expect("wal metadata").len();
        assert!(len_after >= WAL_HEADER.len() as u64);
    }

    #[test]
    fn complete_crc_mismatch_is_corruption() {
        let dir = TempDir::new().expect("temp dir");

        {
            let mut storage = open_storage(&dir).expect("open file storage");
            storage.append_entries(&[entry(1, b"one")]).expect("append");
            storage.sync().expect("sync");
        }

        let mut bytes = fs::read(wal_path(&dir)).expect("read wal");
        let payload_offset = WAL_HEADER.len() + 8;
        bytes[payload_offset] ^= 0x01;
        fs::write(wal_path(&dir), bytes).expect("write corrupted wal");

        let err = open_storage(&dir).expect_err("corruption should fail");
        assert!(matches!(err, StorageError::Corruption(_)));
    }
}
