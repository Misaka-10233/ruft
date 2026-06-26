use futures::StreamExt;
use std::cmp::min;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::events::event::Event::{self, VoteResult};
use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::client::{NodeId, RuftClient};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};
use crate::ruft::role::Role::{self};
use crate::ruft::server::RuftServer;
use crate::storage::{FileStorage, HardState, MemoryStorage, Storage, StorageResult};
use crate::utilis::types::LogEntry;
use log::{error, warn};
use tarpc::context;

use tokio::sync::mpsc;

use crate::utilis::timer::Timer;

// 单个 Raft 节点：所有状态修改都经由事件循环串行执行，避免并发写状态。
// A single Raft node: all state mutations flow through the event loop serially.
pub struct Ruft {
    // 节点 ID。
    // Node ID.
    node_id: u64,
    // RPC 客户端集合。
    // RPC client set.
    rpc_client: Arc<RuftClient>,
    // 全部节点列表。
    // Full cluster membership.
    members: Arc<Vec<NodeId>>,

    // 当前节点角色。
    // Current node role.
    role: Role,

    // 持久化通用状态，真实实现中应在 RPC 响应前落盘。
    // Persistent Raft state; a real implementation must persist before replying to RPCs.
    storage: Box<dyn Storage>,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>, // log[0] 为空哨兵日志。log[0] is an empty sentinel entry.

    // 临时通用状态：已提交索引和已应用索引。
    // Volatile shared state: committed index and applied index.
    commit_index: u64,
    last_applied: u64,

    // Leader 专用临时状态，每次成为 Leader 后重新初始化。
    // Leader-only volatile state, reinitialized after each election.
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    // 事件管道，连接 RPC、计时器和本地命令。
    // Event channel connecting RPCs, timers, and local commands.
    event_sender: mpsc::Sender<Event>,
    event_receiver: mpsc::Receiver<Event>,

    // 选举计时器。
    // Election timer.
    election_timer: Timer,

    // Apply 管道：把已提交日志交给调用方状态机。
    // Apply channel: exposes committed log entries to the caller's state machine.
    apply_sender: mpsc::UnboundedSender<Vec<u8>>,
    pub apply: mpsc::UnboundedReceiver<Vec<u8>>, // 暴露给调用者的 Apply Receiver。Receiver exposed to callers.
}

// 可克隆的运行时句柄：节点进入 run 后，调用方仍可提交日志和读取状态快照。
// Cloneable runtime handle: callers can submit logs and inspect snapshots after run starts.
#[derive(Clone)]
pub struct RuftHandle {
    event_sender: mpsc::Sender<Event>,
}

// 节点状态快照：只读观测数据，不暴露可变 Raft 内部结构。
// Node state snapshot: read-only observations without exposing mutable internals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuftSnapshot {
    pub node_id: NodeId,
    pub role: Role,
    pub current_term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_len: usize,
}

impl Ruft {
    // 根据持久化开关选择存储层并恢复节点状态。
    // Selects a storage backend from the durability flag, then restores node state.
    pub fn new(
        node_id: NodeId,
        rpc_client: RuftClient,
        members: Vec<NodeId>,
        persistent: bool,
        election_timeout_lower_bound_ms: u32,
        election_timeout_upper_bound_ms: u32,
    ) -> StorageResult<Self> {
        let storage: Box<dyn Storage> = if persistent {
            Box::new(FileStorage::open(Self::default_storage_dir(node_id))?)
        } else {
            Box::new(MemoryStorage::new())
        };

        Self::new_with_storage(
            node_id,
            rpc_client,
            members,
            storage,
            election_timeout_lower_bound_ms,
            election_timeout_upper_bound_ms,
        )
    }

    fn default_storage_dir(node_id: NodeId) -> PathBuf {
        PathBuf::from("ruft-data").join(format!("node-{node_id}"))
    }

    // 从已选定的存储层恢复节点状态；日志从哨兵项开始，便于使用 1-based Raft 索引。
    // Restores node state from a selected storage backend; the sentinel log entry keeps 1-based indexes convenient.
    fn new_with_storage(
        node_id: NodeId,
        rpc_client: RuftClient,
        members: Vec<NodeId>,
        storage: Box<dyn Storage>,
        election_timeout_lower_bound_ms: u32,
        election_timeout_upper_bound_ms: u32,
    ) -> StorageResult<Self> {
        let (event_sender, event_receiver) = mpsc::channel::<Event>(1024);
        let (apply_sender, apply) = mpsc::unbounded_channel::<Vec<u8>>();
        let storage_state = storage.load()?;
        let log_len = storage_state.log.len() as u64;

        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for id in members.iter().copied().filter(|id| *id != node_id) {
            next_index.insert(id, log_len);
            match_index.insert(id, 0);
        }

        Ok(Self {
            node_id,
            rpc_client: Arc::new(rpc_client),
            members: Arc::new(members),
            role: Role::Follower,
            storage,
            current_term: storage_state.hard_state.current_term,
            voted_for: storage_state.hard_state.voted_for,
            log: storage_state.log,
            commit_index: 0,
            last_applied: 0,
            next_index,
            match_index,
            event_sender,
            event_receiver,
            election_timer: Timer::new(
                election_timeout_lower_bound_ms,
                election_timeout_upper_bound_ms,
            ),
            apply_sender,
            apply,
        })
    }

    // 主事件循环：把计时器 tick 和内部事件统一调度到状态机处理。
    // Main event loop: routes timer ticks and internal events into the state machine.
    pub async fn run(&mut self) {
        let heartbeat_sender = self.event_sender.clone();
        // 独立心跳节拍；非 Leader 收到 Heartbeat 事件后会直接忽略。
        // Independent heartbeat ticker; non-leaders ignore Heartbeat events.
        _ = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                match heartbeat_sender.send(Event::Heartbeat).await {
                    Ok(_) => {}
                    Err(err) => error!("Heartbeat error: {err}"),
                }
            }
        });
        loop {
            tokio::select! {
                tick = self.election_timer.tick.recv() => {
                    if tick.is_none() {
                        warn!("Election timer channel closed");
                        break;
                    }
                    if let Err(err) = self.event_sender.send(Event::ElectionTimeout).await {
                        error!("Error sending ElectionTimeout event: {err}");
                        break;
                    }
                },
                event = self.event_receiver.recv() => {
                    match event {
                        Some(event) => self._handle_event(event).await,
                        None => {
                            warn!("Event receiver closed");
                            break;
                        }
                    }
                },
            }
        }
    }

    // 暴露给调用者的写入入口，真正追加前会先进入事件循环。
    // Public write entry; the command enters the event loop before being appended.
    pub async fn append_log(
        &self,
        command: Vec<u8>,
    ) -> Result<bool, tokio::sync::oneshot::error::RecvError> {
        self.handle().append_log(command).await
    }

    // 创建运行时句柄；可在节点被后台任务持有后继续从测试或上层代码驱动它。
    // Creates a runtime handle usable after the node is moved into a background task.
    pub fn handle(&self) -> RuftHandle {
        RuftHandle {
            event_sender: self.event_sender.clone(),
        }
    }

    // 取走 apply 接收端，便于调用方把节点移动到 run 任务前保留应用输出。
    // Takes the apply receiver so callers can keep it before moving the node into run.
    pub fn take_apply(&mut self) -> mpsc::UnboundedReceiver<Vec<u8>> {
        let (_sender, receiver) = mpsc::unbounded_channel();
        std::mem::replace(&mut self.apply, receiver)
    }

    // 返回当前状态快照；该方法在事件循环内调用时是串行一致的。
    // Returns a current state snapshot; called inside the event loop it is serially consistent.
    pub fn snapshot(&self) -> RuftSnapshot {
        RuftSnapshot {
            node_id: self.node_id,
            role: self.role,
            current_term: self.current_term,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            log_len: self.log.len(),
        }
    }

    // 创建网络服务句柄；tarpc server 应持有该轻量句柄，而不是直接持有 Ruft 状态机。
    // Creates the network service handle; tarpc servers should hold this light handle, not Ruft itself.
    pub fn server(&self) -> RuftServer {
        RuftServer::new(self.node_id, self.event_sender.clone())
    }

    // 事件分发中心：所有分支都只在这里改变节点状态。
    // Event dispatcher: every branch mutates node state only through this path.
    async fn _handle_event(&mut self, event: Event) {
        match event {
            Event::AppendEntries(_args, _reply) => {
                let reply = self._handle_append_entries(_args).await;
                match _reply.send(reply) {
                    Ok(_) => {}
                    Err(err) => {
                        error!("Error sending AppendEntries reply: {err:?}");
                    }
                }
            }
            Event::RequestVote(_args, _reply) => {
                let reply = self._handle_request_vote(_args);
                match _reply.send(reply) {
                    Ok(_) => {}
                    Err(err) => {
                        error!("Error sending RequestVote reply: {err:?}");
                    }
                }
            }
            Event::Apply => {
                self._apply_committed();
            }
            Event::ElectionTimeout => {
                if self.role != Role::Leader {
                    self._become_candidate();
                }
            }

            Event::VoteResult(success, new_term) => {
                self._on_vote_result(success, new_term).await;
            }
            Event::ReceiveEnoughVotes => {
                self._become_leader();
            }
            Event::ShouldBeFollower(term) => {
                self._become_follower(term);
            }
            Event::Heartbeat => {
                self._on_heartbeat();
            }
            Event::NewLogEntries(log, reply) => {
                let accepted = self._on_append_log(log);
                if let Err(err) = reply.send(accepted) {
                    error!("Error sending NewLogEntries reply: {err:?}");
                }
            }
            Event::Snapshot(reply) => {
                if let Err(err) = reply.send(self.snapshot()) {
                    error!("Error sending Snapshot reply: {err:?}");
                }
            }
            Event::AppendEntriesReply(reply) => {
                self._on_append_entries_reply(reply).await;
            }
        }
    }

    // 向单个 Follower 发送当前需要的 AppendEntries。
    // Sends the currently needed AppendEntries request to one follower.
    async fn _call_append_entries(&mut self, id: NodeId) {
        if self.role != Role::Leader {
            return;
        }
        let ctx = tarpc::context::Context::current();
        let args = self._generate_append_entries_args(id);

        match self.rpc_client.call_append_entries(id, ctx, args).await {
            Ok(reply) => {
                if let Err(err) = self
                    .event_sender
                    .send(Event::AppendEntriesReply(reply))
                    .await
                {
                    error!("Error sending AppendEntriesReply event: {err}");
                }
            }
            Err(err) => error!("Error calling AppendEntries on node {id}: {err}"),
        }
    }

    // 向所有 Follower 广播 AppendEntries；每个节点按自己的 next_index 生成参数。
    // Broadcasts AppendEntries to followers; each peer gets args based on its next_index.
    fn _boardcast_append_entries(&self) {
        if self.role != Role::Leader {
            return;
        }
        let ctx = tarpc::context::Context::current();
        let sender = self.event_sender.clone();

        let members = self.members.clone();
        let client = self.rpc_client.clone();
        let me = self.node_id;

        // 先在当前线程构建参数，避免后台任务直接读取可变 Raft 状态。
        // Build args before spawning so the task does not read mutable Raft state.
        let mut args_list: Vec<(u64, AppendEntriesArgs)> = Vec::new();
        for id in members.iter() {
            if *id == me {
                continue;
            }
            args_list.push((*id, self._generate_append_entries_args(*id)));
        }

        tokio::spawn(async move {
            for (id, args) in args_list.into_iter() {
                match client.call_append_entries(id, ctx, args).await {
                    Ok(reply) => {
                        if let Err(err) = sender.send(Event::AppendEntriesReply(reply)).await {
                            error!("Error sending AppendEntriesReply event: {err}");
                            break;
                        }
                    }
                    Err(err) => error!("Error calling AppendEntries on node {id}: {err}"),
                }
            }
        });
    }

    // 持久化 hard state 并立即刷盘；成功后调用方才能更新内存状态。
    // Persists hard state and flushes immediately; callers update memory only after success.
    fn persist_hard_state(
        &mut self,
        current_term: u64,
        voted_for: Option<NodeId>,
    ) -> StorageResult<()> {
        self.storage.save_hard_state(HardState {
            current_term,
            voted_for,
        })?;
        self.storage.sync()
    }

    // 追加持久化日志并刷盘；Leader 本地接收新命令时使用。
    // Appends persistent log entries and flushes; used when a leader accepts local commands.
    fn append_persistent_entries(&mut self, entries: &[LogEntry]) -> StorageResult<()> {
        self.storage.append_entries(entries)?;
        self.storage.sync()
    }

    // 原子替换持久化日志后缀并刷盘；Follower 处理 Leader 覆盖时使用。
    // Atomically replaces a persistent log suffix and flushes; used for follower conflict repair.
    fn replace_persistent_suffix(
        &mut self,
        first_index_to_remove: u64,
        entries: &[LogEntry],
    ) -> StorageResult<()> {
        self.storage
            .replace_suffix(first_index_to_remove, entries)?;
        self.storage.sync()
    }

    // 处理 Leader 的心跳或日志复制请求，实现任期检查、日志匹配和提交推进。
    // Handles leader heartbeats/replication: term checks, log matching, and commit advance.
    async fn _handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term < self.current_term {
            return AppendEntriesReply {
                node_id: self.node_id,
                term: self.current_term,
                success: false,
                match_index: 0,
            }; // 过期任期，拒绝。Stale term, reject.
        }

        // 遇到更高任期时必须立刻更新并退回 Follower。
        // A higher term must be adopted immediately, stepping down to follower.
        if args.term > self.current_term {
            if let Err(err) = self._refresh_term(args.term) {
                error!("Error persisting higher term from AppendEntries: {err}");
                return AppendEntriesReply {
                    node_id: self.node_id,
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                };
            }
        }

        let mut reply = AppendEntriesReply {
            node_id: self.node_id,
            term: self.current_term,
            success: false,
            match_index: 0,
        };

        // 合法 Leader 出现时，Candidate/Leader 都必须让位。
        // When a valid leader appears, candidate/leader must step down.
        if self.role != Role::Follower {
            self._become_follower(args.term);
        }

        // 收到合法 Leader 消息后延后下一次选举超时。
        // A valid leader message postpones the next election timeout.
        self._reset_election_timer();

        // Raft 日志一致性检查：前一条日志必须同时匹配索引和任期。
        // Raft log consistency check: previous entry must match by index and term.
        let log_len = self.log.len() as u64;
        if args.prev_log_index >= log_len {
            return reply; // 索引超出日志范围。Index is beyond local log.
        }
        if args.prev_log_term != self.log[args.prev_log_index as usize].term {
            return reply; // 日志任期不匹配。Previous log term does not match.
        }

        // 匹配点之后以 Leader 为准，删除冲突条目并追加新条目。
        // After the matching point, follow the leader by truncating conflicts and appending.
        let first_new_index = args.prev_log_index + 1;
        let mut replace_from = None;
        for (offset, entry) in args.entries.iter().enumerate() {
            let index = first_new_index + offset as u64;
            if index >= self.log.len() as u64 || self.log[index as usize].term != entry.term {
                replace_from = Some(index);
                break;
            }
        }

        if let Some(index) = replace_from {
            let entries_start = (index - first_new_index) as usize;
            let new_entries = args.entries[entries_start..].to_vec();
            if let Err(err) = self.replace_persistent_suffix(index, &new_entries) {
                error!("Error persisting AppendEntries log replacement: {err}");
                return reply;
            }
            self.log.truncate(index as usize);
            self.log.extend(new_entries);
        }

        // Follower 的提交进度不能超过本地已有的最后一条日志。
        // Follower commit index must not pass the last local log entry.
        if args.leader_commit > self.commit_index {
            let last_new_index = (self.log.len() - 1) as u64;
            self.commit_index = min(args.leader_commit, last_new_index);
            self._apply_committed();
        }
        reply.success = true;
        reply.match_index = (self.log.len() - 1) as u64;
        reply
    }

    // 处理投票请求：候选人任期够新、日志够新且本任期未投过票才会同意。
    // Handles vote requests: grant only for fresh-enough term/log and no conflicting vote.
    fn _handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if args.term > self.current_term {
            if let Err(err) = self._refresh_term(args.term) {
                error!("Error persisting higher term from RequestVote: {err}");
                return RequestVoteReply {
                    term: self.current_term,
                    vote_granted: false,
                };
            }
        }

        let mut reply = RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
        };

        let my_last_log_index = self.log.len() as u64 - 1;
        let my_last_log_term = self.log[my_last_log_index as usize].term;

        // 日志新旧比较先看最后任期，再看最后索引。
        // Log freshness compares last term first, then last index.
        let log_is_up_to_date = args.last_log_term > my_last_log_term
            || (args.last_log_term == my_last_log_term && args.last_log_index >= my_last_log_index);

        // 同一任期最多投给一个 Candidate，但允许重复响应同一个 Candidate。
        // One vote per term, while repeated requests from the same candidate are allowed.
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);

        if can_vote && log_is_up_to_date {
            if let Err(err) = self.persist_hard_state(self.current_term, Some(args.candidate_id)) {
                error!(
                    "Error persisting vote for candidate {}: {err}",
                    args.candidate_id
                );
                return reply;
            }
            self.voted_for = Some(args.candidate_id);
            reply.vote_granted = true;

            // 投票后重置计时器，给 Candidate 收集多数票的时间。
            // Reset after voting to give the candidate time to collect a majority.
            self._reset_election_timer();
        }
        reply
    }

    // 处理客户端新日志；Leader 先追加本地日志，再等待多数派复制后提交。
    // Handles a new client log; this implementation accepts it only on the leader.
    fn _on_append_log(&mut self, log: Vec<u8>) -> bool {
        // 非 Leader 不能直接接收写入。
        // Non-leaders do not accept writes directly.
        if self.role != Role::Leader {
            return false;
        }

        // 新日志使用当前 Leader 任期。
        // New entries are stamped with the current leader term.
        let entry = LogEntry {
            term: self.current_term,
            command: log,
        };
        if let Err(err) = self.append_persistent_entries(std::slice::from_ref(&entry)) {
            error!("Error persisting leader log entry: {err}");
            return false;
        }
        self.log.push(entry);

        // 立刻广播，减少客户端命令等待下一次心跳的延迟。
        // Broadcast immediately so client commands need not wait for the next heartbeat.
        self._boardcast_append_entries();
        true
    }

    // 选举超时后成为 Candidate：增加任期、投给自己，并向其他节点拉票。
    // On election timeout, become candidate: bump term, vote for self, and request votes.
    fn _become_candidate(&mut self) {
        // 开始新一轮选举前重置超时，避免马上再次触发。
        // Reset timeout before starting the new election round.
        self.election_timer.reset();
        let cur_term = self.current_term + 1;
        if let Err(err) = self.persist_hard_state(cur_term, Some(self.node_id)) {
            error!("Error persisting candidate hard state: {err}");
            return;
        }

        self.role = Role::Candidate;
        self.current_term = cur_term;

        let majority = self.members.len() / 2 + 1;

        // Candidate 自动投给自己。
        // Candidate votes for itself.
        self.voted_for = Some(self.node_id);

        let args = RequestVoteArgs {
            term: cur_term,
            candidate_id: self.node_id,
            last_log_index: self.log.len() as u64 - 1,
            last_log_term: self.log[self.log.len() - 1].term,
        };
        // 并发发起投票，后台汇总结果后再回到事件循环改变状态。
        // Request votes concurrently; the background task reports back via events.
        let mut reply_stream = self
            .rpc_client
            .broadcast_request_vote(context::Context::current(), args);

        let mut vote_count = 1;
        let sender = self.event_sender.clone();
        tokio::spawn(async move {
            while let Some((_, reply)) = reply_stream.next().await {
                if let Ok(reply) = reply {
                    if reply.term > cur_term {
                        if let Err(err) = sender.send(VoteResult(false, reply.term)).await {
                            error!("Error sending VoteResult event: {err}");
                            break;
                        }
                    }
                    if reply.vote_granted {
                        vote_count += 1;
                    }
                    if vote_count >= majority {
                        if let Err(err) = sender.send(VoteResult(true, cur_term)).await {
                            error!("Error sending VoteResult event: {err}");
                        }
                        break;
                    }
                }
            }
        });
    }

    // 退回 Follower；如果发现更高任期，同时清空投票记录。
    // Steps down to follower; a higher term also clears the recorded vote.
    fn _become_follower(&mut self, term: u64) {
        if term > self.current_term {
            if let Err(err) = self._refresh_term(term) {
                error!("Error persisting follower term transition: {err}");
                return;
            }
        }

        self.role = Role::Follower;
    }

    // 多数票达成后成为 Leader，并初始化每个 Follower 的复制进度。
    // Becomes leader after majority votes and initializes follower replication progress.
    fn _become_leader(&mut self) {
        // 只有 Candidate 能晋升，避免重复或非法状态转换。
        // Only candidates may be promoted, preventing duplicate or illegal transitions.
        if self.role == Role::Leader || self.role == Role::Follower {
            return;
        }

        // 新 Leader 假设每个 Follower 下一条待复制日志是本地日志末尾之后。
        // New leader assumes each follower's next entry starts after the local log tail.
        self.role = Role::Leader;
        self.next_index
            .iter_mut()
            .for_each(|x| *x.1 = self.log.len() as u64);
        self.match_index.iter_mut().for_each(|x| *x.1 = 0);
        self._boardcast_append_entries();
    }

    // 接受新的任期，同时回到 Follower 并清空本任期投票。
    // Adopts a new term, returns to follower, and clears the vote for that term.
    fn _refresh_term(&mut self, new_term: u64) -> StorageResult<()> {
        self.persist_hard_state(new_term, None)?;
        self.current_term = new_term;
        self.role = Role::Follower;
        self.voted_for = None;
        Ok(())
    }

    // 心跳节拍到达时，Leader 发送空或非空 AppendEntries。
    // On heartbeat ticks, leaders send empty or non-empty AppendEntries.
    fn _on_heartbeat(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        self._boardcast_append_entries();
    }

    // 处理 Follower 的复制响应：更高任期退位，成功则推进进度，失败则回退重试。
    // Handles follower replication replies: step down on higher term, advance or back off.
    async fn _on_append_entries_reply(&mut self, reply: AppendEntriesReply) {
        if reply.term < self.current_term {
            return;
        }
        if reply.term > self.current_term {
            if let Err(err) = self._refresh_term(reply.term) {
                error!("Error persisting higher term from AppendEntriesReply: {err}");
            }
            return;
        }
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }

        if reply.success {
            // 按 Follower 返回的匹配索引推进复制进度。
            // Advance replication progress using the follower's returned match index.
            *self.match_index.get_mut(&reply.node_id).unwrap() = reply.match_index;
            *self.next_index.get_mut(&reply.node_id).unwrap() = reply.match_index + 1;

            // Leader 只能提交当前任期内、已复制到多数派的日志。
            // Leaders may commit only current-term entries replicated on a majority.
            self._advance_commit_index();
            self._apply_committed();
        } else {
            // 复制失败通常表示 prev_log 不匹配，回退 next_index 后下次重试。
            // Failure usually means prev_log mismatch; decrement next_index and retry later.
            let next_index = self.next_index.get_mut(&reply.node_id).unwrap();
            if *next_index > 1 {
                *next_index -= 1;
            }
        }
    }

    // 根据多数派复制进度推进 Leader 的提交索引。
    // Advances the leader commit index based on majority replication progress.
    fn _advance_commit_index(&mut self) {
        let majority = self.members.len() / 2 + 1;

        for index in (self.commit_index + 1)..self.log.len() as u64 {
            if self.log[index as usize].term != self.current_term {
                continue;
            }

            // Leader 自己天然拥有本地日志，因此计数从 1 开始。
            // The leader already has its local log, so counting starts at 1.
            let replicated_count = 1 + self
                .match_index
                .values()
                .filter(|match_index| **match_index >= index)
                .count();

            if replicated_count >= majority {
                self.commit_index = index;
            }
        }
    }

    // 按日志顺序应用已提交日志，保证状态机观察到的命令顺序和提交顺序一致。
    // Applies committed entries in log order so the state machine observes commit order.
    fn _apply_committed(&mut self) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            match self
                .apply_sender
                .send(self.log[self.last_applied as usize].command.clone())
            {
                Ok(_) => {}
                Err(err) => {
                    error!("Error sending Apply message: {err}");
                }
            }
        }
    }

    // 包装计时器重置，便于统一表达 Raft 语义。
    // Wraps timer reset to keep Raft semantics explicit at call sites.
    fn _reset_election_timer(&mut self) {
        self.election_timer.reset();
    }

    // 按目标 Follower 的 next_index 生成 AppendEntries 参数。
    // Builds AppendEntries args for a follower using that follower's next_index.
    fn _generate_append_entries_args(&self, id: u64) -> AppendEntriesArgs {
        let ni = self.next_index[&id];
        let args = AppendEntriesArgs {
            term: self.current_term,
            leader_id: self.node_id,
            prev_log_index: ni - 1,
            prev_log_term: self.log[(ni - 1) as usize].term,
            entries: self.log[ni as usize..].to_vec(),
            leader_commit: self.commit_index,
        };
        args
    }

    // 处理投票汇总结果：多数票晋升，更高任期或失败则退回 Follower。
    // Handles aggregated vote results: majority promotes, higher term/failure steps down.
    async fn _on_vote_result(&self, success: bool, new_term: u64) {
        if self.role != Role::Candidate {
            return;
        }
        if success {
            if let Err(err) = self.event_sender.send(Event::ReceiveEnoughVotes).await {
                error!("Error sending ReceiveEnoughVotes event: {err}");
            }
        } else {
            if let Err(err) = self
                .event_sender
                .send(Event::ShouldBeFollower(new_term))
                .await
            {
                error!("Error sending ShouldBeFollower event: {err}");
            }
        }
    }
}

impl RuftHandle {
    // 向节点提交日志；返回 true 表示当前节点是 Leader 并接受该命令。
    // Submits a log; true means this node is currently leader and accepted it.
    pub async fn append_log(
        &self,
        command: Vec<u8>,
    ) -> Result<bool, tokio::sync::oneshot::error::RecvError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .event_sender
            .send(Event::NewLogEntries(command, tx))
            .await
            .is_err()
        {
            return Err(rx.await.unwrap_err());
        }
        rx.await
    }

    // 获取节点状态快照。
    // Gets a node state snapshot.
    pub async fn snapshot(&self) -> Result<RuftSnapshot, tokio::sync::oneshot::error::RecvError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.event_sender.send(Event::Snapshot(tx)).await.is_err() {
            return Err(rx.await.unwrap_err());
        }
        rx.await
    }
}

#[cfg(test)]
// 单元测试覆盖核心 Raft 状态转换和 RPC 判断，不依赖真实网络。
// Unit tests cover core Raft transitions and RPC decisions without real networking.
mod tests {
    use super::*;
    use crate::rpc::client::RuftClient;
    use crate::storage::{FileStorage, MemoryStorage, StorageState};
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
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
                "ruft-node-storage-{}-{nanos}-{seq}",
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

    fn new_test_node(node_id: NodeId, members: Vec<NodeId>) -> Ruft {
        Ruft::new(
            node_id,
            RuftClient::new(node_id, HashMap::new()),
            members,
            false,
            150,
            300,
        )
        .expect("load memory-backed node")
    }

    fn entry(term: u64, command: &[u8]) -> LogEntry {
        LogEntry {
            term,
            command: command.to_vec(),
        }
    }

    fn test_node_with_storage(
        node_id: NodeId,
        members: Vec<NodeId>,
        storage: MemoryStorage,
    ) -> Ruft {
        Ruft::new_with_storage(
            node_id,
            RuftClient::new(node_id, HashMap::new()),
            members,
            Box::new(storage),
            150,
            300,
        )
        .expect("load test storage")
    }

    fn test_node_with_file_storage(
        node_id: NodeId,
        members: Vec<NodeId>,
        storage: FileStorage,
    ) -> Ruft {
        Ruft::new_with_storage(
            node_id,
            RuftClient::new(node_id, HashMap::new()),
            members,
            Box::new(storage),
            150,
            300,
        )
        .expect("load file storage")
    }

    #[tokio::test]
    async fn new_initializes_follower_with_sentinel_log() {
        let node = Ruft::new(
            1,
            RuftClient::new(1, HashMap::new()),
            vec![1, 2, 3],
            false,
            150,
            300,
        )
        .expect("load memory-backed node");

        assert_eq!(node.node_id, 1);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.voted_for, None);
        assert_eq!(node.commit_index, 0);
        assert_eq!(node.last_applied, 0);
        assert_eq!(node.log.len(), 1);
        assert_eq!(node.log[0].term, 0);
        assert!(node.log[0].command.is_empty());
        assert_eq!(node.next_index[&2], 1);
        assert_eq!(node.next_index[&3], 1);
        assert_eq!(node.match_index[&2], 0);
        assert_eq!(node.match_index[&3], 0);
        assert!(!node.next_index.contains_key(&1));
        assert!(!node.match_index.contains_key(&1));
    }

    #[tokio::test]
    async fn new_restores_persistent_state_from_storage() {
        let storage = MemoryStorage::with_state(StorageState {
            hard_state: HardState {
                current_term: 4,
                voted_for: Some(3),
            },
            log: vec![entry(0, b""), entry(2, b"old"), entry(4, b"new")],
        })
        .expect("valid storage state");

        let node = test_node_with_storage(1, vec![1, 2, 3], storage);

        assert_eq!(node.current_term, 4);
        assert_eq!(node.voted_for, Some(3));
        assert_eq!(node.log.len(), 3);
        assert_eq!(node.next_index[&2], 3);
        assert_eq!(node.next_index[&3], 3);
    }

    #[tokio::test]
    async fn new_with_persistent_flag_restores_from_default_storage_dir() {
        let node_id = 10_001;
        let storage_dir = Ruft::default_storage_dir(node_id);
        let _ = fs::remove_dir_all(&storage_dir);

        {
            let mut storage = FileStorage::open(&storage_dir).expect("open default file storage");
            storage
                .save_hard_state(HardState {
                    current_term: 6,
                    voted_for: Some(2),
                })
                .expect("save hard state");
            storage
                .append_entries(&[entry(3, b"default")])
                .expect("append entries");
            storage.sync().expect("sync");
        }

        let node = Ruft::new(
            node_id,
            RuftClient::new(node_id, HashMap::new()),
            vec![node_id, 2],
            true,
            150,
            300,
        )
        .expect("load persistent node from default dir");

        assert_eq!(node.current_term, 6);
        assert_eq!(node.voted_for, Some(2));
        assert_eq!(node.log, vec![entry(0, b""), entry(3, b"default")]);
        assert_eq!(node.next_index[&2], 2);

        let _ = fs::remove_dir_all(&storage_dir);
    }

    #[tokio::test]
    async fn new_restores_persistent_state_from_file_storage() {
        let dir = TempDir::new().expect("temp dir");
        {
            let mut storage = FileStorage::open(dir.path()).expect("open file storage");
            storage
                .save_hard_state(HardState {
                    current_term: 4,
                    voted_for: Some(3),
                })
                .expect("save hard state");
            storage
                .append_entries(&[entry(2, b"old"), entry(4, b"new")])
                .expect("append entries");
            storage.sync().expect("sync");
        }

        let storage = FileStorage::open(dir.path()).expect("reopen file storage");
        let node = test_node_with_file_storage(1, vec![1, 2, 3], storage);

        assert_eq!(node.current_term, 4);
        assert_eq!(node.voted_for, Some(3));
        assert_eq!(node.log.len(), 3);
        assert_eq!(node.next_index[&2], 3);
        assert_eq!(node.next_index[&3], 3);
    }

    #[tokio::test]
    async fn leader_append_log_persists_entry_before_accepting() {
        let storage = MemoryStorage::with_state(StorageState {
            hard_state: HardState {
                current_term: 1,
                voted_for: None,
            },
            log: vec![entry(0, b"")],
        })
        .expect("valid storage state");
        let mut node = test_node_with_storage(1, vec![1, 2, 3], storage);
        node.role = Role::Leader;

        assert!(node._on_append_log(b"entry".to_vec()));

        let persisted = node.storage.load().expect("load storage");
        assert_eq!(persisted.log, vec![entry(0, b""), entry(1, b"entry")]);
        assert_eq!(node.log, persisted.log);
    }

    #[tokio::test]
    async fn leader_append_log_persists_entry_to_file_storage() {
        let dir = TempDir::new().expect("temp dir");
        let storage = FileStorage::open(dir.path()).expect("open file storage");
        let mut node = test_node_with_file_storage(1, vec![1, 2, 3], storage);
        node.current_term = 1;
        node.role = Role::Leader;

        assert!(node._on_append_log(b"entry".to_vec()));
        drop(node);

        let storage = FileStorage::open(dir.path()).expect("reopen file storage");
        assert_eq!(
            storage.load().expect("load storage").log,
            vec![entry(0, b""), entry(1, b"entry")]
        );
    }

    #[tokio::test]
    async fn append_entries_conflict_replacement_persists_storage_and_memory() {
        let storage = MemoryStorage::with_state(StorageState {
            hard_state: HardState {
                current_term: 1,
                voted_for: None,
            },
            log: vec![entry(0, b""), entry(1, b"keep"), entry(1, b"stale")],
        })
        .expect("valid storage state");
        let mut node = test_node_with_storage(1, vec![1, 2, 3], storage);

        let reply = node
            ._handle_append_entries(AppendEntriesArgs {
                term: 2,
                leader_id: 2,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![entry(2, b"replace")],
                leader_commit: 0,
            })
            .await;

        assert!(reply.success);
        let expected = vec![entry(0, b""), entry(1, b"keep"), entry(2, b"replace")];
        assert_eq!(node.log, expected);
        assert_eq!(node.storage.load().expect("load storage").log, expected);
    }

    #[tokio::test]
    async fn append_entries_conflict_replacement_persists_to_file_storage() {
        let dir = TempDir::new().expect("temp dir");
        {
            let mut storage = FileStorage::open(dir.path()).expect("open file storage");
            storage
                .save_hard_state(HardState {
                    current_term: 1,
                    voted_for: None,
                })
                .expect("save hard state");
            storage
                .append_entries(&[entry(1, b"keep"), entry(1, b"stale")])
                .expect("append entries");
            storage.sync().expect("sync");
        }

        let storage = FileStorage::open(dir.path()).expect("reopen file storage");
        let mut node = test_node_with_file_storage(1, vec![1, 2, 3], storage);
        let reply = node
            ._handle_append_entries(AppendEntriesArgs {
                term: 2,
                leader_id: 2,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![entry(2, b"replace")],
                leader_commit: 0,
            })
            .await;

        assert!(reply.success);
        drop(node);

        let storage = FileStorage::open(dir.path()).expect("reopen file storage");
        assert_eq!(
            storage.load().expect("load storage").log,
            vec![entry(0, b""), entry(1, b"keep"), entry(2, b"replace")]
        );
    }

    #[tokio::test]
    async fn request_vote_persists_granted_vote() {
        let mut node = new_test_node(1, vec![1, 2, 3]);

        let reply = node._handle_request_vote(RequestVoteArgs {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        });

        assert!(reply.vote_granted);
        let persisted = node.storage.load().expect("load storage");
        assert_eq!(persisted.hard_state.current_term, 1);
        assert_eq!(persisted.hard_state.voted_for, Some(2));
    }

    #[tokio::test]
    async fn candidate_self_vote_is_persisted() {
        let mut node = new_test_node(1, vec![1]);

        node._become_candidate();

        let persisted = node.storage.load().expect("load storage");
        assert_eq!(persisted.hard_state.current_term, 1);
        assert_eq!(persisted.hard_state.voted_for, Some(1));
    }

    #[tokio::test]
    async fn request_vote_grants_vote_for_newer_term_and_updates_term() {
        let mut node = new_test_node(1, vec![1, 2, 3]);

        let reply = node._handle_request_vote(RequestVoteArgs {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        });

        assert!(reply.vote_granted);
        assert_eq!(reply.term, 1);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.voted_for, Some(2));
    }

    #[tokio::test]
    async fn request_vote_rejects_stale_term_without_changing_vote() {
        let mut node = new_test_node(1, vec![1, 2, 3]);
        node.current_term = 3;
        node.voted_for = Some(3);

        let reply = node._handle_request_vote(RequestVoteArgs {
            term: 2,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        });

        assert!(!reply.vote_granted);
        assert_eq!(reply.term, 3);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.voted_for, Some(3));
    }

    #[tokio::test]
    async fn request_vote_rejects_candidate_with_stale_log() {
        let mut node = new_test_node(1, vec![1, 2, 3]);
        node.log.push(LogEntry {
            term: 2,
            command: b"local".to_vec(),
        });

        let reply = node._handle_request_vote(RequestVoteArgs {
            term: 3,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 1,
        });

        assert!(!reply.vote_granted);
        assert_eq!(reply.term, 3);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.voted_for, None);
    }

    #[tokio::test]
    async fn append_entries_accepts_matching_sentinel_and_appends_entries() {
        let mut node = new_test_node(1, vec![1, 2, 3]);

        let reply = node
            ._handle_append_entries(AppendEntriesArgs {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 1,
                    command: b"set x".to_vec(),
                }],
                leader_commit: 1,
            })
            .await;

        assert!(reply.success);
        assert_eq!(reply.term, 1);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.log.len(), 2);
        assert_eq!(node.log[1].term, 1);
        assert_eq!(node.log[1].command, b"set x".to_vec());
        assert_eq!(node.commit_index, 1);
    }

    #[tokio::test]
    async fn append_entries_rejects_stale_term() {
        let mut node = new_test_node(1, vec![1, 2, 3]);
        node.current_term = 2;

        let reply = node
            ._handle_append_entries(AppendEntriesArgs {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 1,
                    command: b"stale".to_vec(),
                }],
                leader_commit: 1,
            })
            .await;

        assert!(!reply.success);
        assert_eq!(reply.term, 2);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.log.len(), 1);
    }

    #[tokio::test]
    async fn election_timeout_moves_follower_to_candidate_and_votes_for_self() {
        let mut node = new_test_node(1, vec![1]);

        node._become_candidate();

        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(1));
    }

    #[tokio::test]
    async fn candidate_becomes_leader_and_initializes_replication_state() {
        let mut node = new_test_node(1, vec![1, 2, 3]);
        node.log.push(LogEntry {
            term: 1,
            command: b"entry".to_vec(),
        });
        node.role = Role::Candidate;
        node.current_term = 1;

        node._become_leader();

        assert_eq!(node.role, Role::Leader);
        assert_eq!(node.next_index[&2], 2);
        assert_eq!(node.next_index[&3], 2);
        assert_eq!(node.match_index[&2], 0);
        assert_eq!(node.match_index[&3], 0);
    }

    #[tokio::test]
    async fn leader_steps_down_on_append_entries_reply_with_higher_term() {
        let mut node = new_test_node(1, vec![1, 2, 3]);
        node.role = Role::Leader;
        node.current_term = 2;

        node._on_append_entries_reply(AppendEntriesReply {
            node_id: 2,
            term: 3,
            success: false,
            match_index: 0,
        })
        .await;

        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.voted_for, None);
    }

    #[tokio::test]
    async fn leader_append_log_waits_for_majority_before_commit() {
        let mut node = new_test_node(1, vec![1, 2, 3]);
        node.role = Role::Leader;
        node.current_term = 1;
        node._become_leader();

        node._on_append_log(b"entry".to_vec());

        assert_eq!(node.log.len(), 2);
        assert_eq!(node.log[1].term, 1);
        assert_eq!(node.log[1].command, b"entry".to_vec());
        assert_eq!(node.commit_index, 0);

        node._on_append_entries_reply(AppendEntriesReply {
            node_id: 2,
            term: 1,
            success: true,
            match_index: 1,
        })
        .await;

        assert_eq!(node.match_index[&2], 1);
        assert_eq!(node.next_index[&2], 2);
        assert_eq!(node.commit_index, 1);
    }

    #[tokio::test]
    async fn leader_does_not_commit_previous_term_entry_by_counting_majority() {
        let mut node = new_test_node(1, vec![1, 2, 3]);
        node.role = Role::Leader;
        node.current_term = 2;
        node.log.push(LogEntry {
            term: 1,
            command: b"old".to_vec(),
        });
        node._become_leader();

        node._on_append_entries_reply(AppendEntriesReply {
            node_id: 2,
            term: 2,
            success: true,
            match_index: 1,
        })
        .await;

        assert_eq!(node.commit_index, 0);
    }
}
