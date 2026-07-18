#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    // Leader 处理客户端写入并复制日志。
    Leader,
    // Follower 被动响应 Leader 和 Candidate。
    Follower,
    // Candidate 发起选举并等待多数票。
    Candidate,
}
