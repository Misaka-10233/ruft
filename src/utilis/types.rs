#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,        // 任期
    pub command: Vec<u8>, // 命令
}
