#[derive(Debug, PartialEq, Eq)]
pub enum AppendResult {
    NotLeader,
    PersistentError,
    Accepted { index: u64, term: u64 },
}
