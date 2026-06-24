use futures::StreamExt;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::events::event::Event::{self, VoteResult};
use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::client::{NodeId, RuftClient};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};
use crate::rpc::rpc::Rpc;
use crate::ruft::role::Role::{self};
use crate::utilis::types::LogEntry;
use log::{error, warn};
use tarpc::context;
use tokio::sync::mpsc::error::SendError;

use tokio::sync::mpsc;

use crate::utilis::timer::Timer;

type SenderResult = Result<(), SendError<Event>>;

pub struct Ruft {
    // 节点ID
    node_id: u64,
    // RPC 客户端
    rpc_client: Arc<RuftClient>,
    // 全部节点列表
    members: Arc<Vec<NodeId>>,

    // 节点角色
    role: Role,

    // 持久化通用状态，RPC响应前持久化
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>, // log[0]为空哨兵日志

    // 临时通用状态
    commit_index: u64,
    last_applied: u64,

    // 作为Leader的临时状态，每次被选举为Leader后重新初始化
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    // 事件管道
    event_sender: mpsc::Sender<Event>,
    event_receiver: mpsc::Receiver<Event>,

    // 选举计时器
    election_timer: Timer,

    // Apply 管道
    apply_sender: mpsc::UnboundedSender<Vec<u8>>,
    pub apply: mpsc::UnboundedReceiver<Vec<u8>>, // 暴露给调用者的 Apply Receiver
}

impl Ruft {
    pub fn new(
        node_id: NodeId,
        rpc_client: RuftClient,
        members: Vec<NodeId>,
        election_timeout_lower_bound_ms: u32,
        election_timeout_upper_bound_ms: u32,
    ) -> Self {
        let (event_sender, event_receiver) = mpsc::channel::<Event>(1024);
        let (apply_sender, apply) = mpsc::unbounded_channel::<Vec<u8>>();

        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for id in members.iter().copied().filter(|id| *id != node_id) {
            next_index.insert(id, 1);
            match_index.insert(id, 0);
        }

        Self {
            node_id,
            rpc_client: Arc::new(rpc_client),
            members: Arc::new(members),
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                command: Vec::new(),
            }],
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
        }
    }
    pub async fn run(&mut self) {
        let heartbeat_sender = self.event_sender.clone();
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

    // 暴露给调用者的方法，用于新增日志
    pub async fn append_log(&mut self, command: Vec<u8>) -> SenderResult {
        self.event_sender.send(Event::NewLogEntries(command)).await
    }

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
            Event::ElectionTimeout => {
                self._become_candidate();
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
            Event::NewLogEntries(log) => {
                self._on_append_log(log);
            }
            Event::AppendEntriesReply(reply) => {
                self._on_append_entries_reply(reply).await;
            }
        }
    }

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

    fn _boardcast_append_entries(&self) {
        if self.role != Role::Leader {
            return;
        }
        let ctx = tarpc::context::Context::current();
        let sender = self.event_sender.clone();

        let members = self.members.clone();
        let client = self.rpc_client.clone();
        let me = self.node_id;

        // 构建AppendEntriesArgs
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

    async fn _handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term < self.current_term {
            return AppendEntriesReply {
                node_id: self.node_id,
                term: self.current_term,
                success: false,
            }; // 过时消息
        }

        // 更新任期
        if args.term > self.current_term {
            self._refresh_term(args.term);
        }

        let mut reply = AppendEntriesReply {
            node_id: self.node_id,
            term: self.current_term,
            success: false,
        };

        // 当自身是Candidate或Leader时，重新成为Follower
        if self.role != Role::Follower {
            if let Err(err) = self
                .event_sender
                .send(Event::ShouldBeFollower(args.term))
                .await
            {
                error!("Error sending ShouldBeFollower event: {err}");
            }
        }

        // 重置选举计时器
        self._reset_election_timer();

        // 匹配日志
        let log_len = self.log.len() as u64;
        if args.prev_log_index >= log_len {
            return reply; // 索引超出日志范围
        }
        if args.prev_log_term != self.log[args.prev_log_index as usize].term {
            return reply; // 日志任期不匹配
        }

        // 覆盖日志
        self.log.truncate(args.prev_log_index as usize + 1);
        self.log.extend(args.entries);

        // 更新提交索引
        if args.leader_commit > self.commit_index {
            let last_new_index = (self.log.len() - 1) as u64;
            self.commit_index = min(args.leader_commit, last_new_index);
        }
        reply.success = true;
        reply
    }

    fn _handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if args.term > self.current_term {
            self._refresh_term(args.term);
        }

        let mut reply = RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
        };

        let my_last_log_index = self.log.len() as u64 - 1;
        let my_last_log_term = self.log[my_last_log_index as usize].term;

        // 日志时效性
        let log_is_up_to_date = args.last_log_term > my_last_log_term
            || (args.last_log_term == my_last_log_term && args.last_log_index >= my_last_log_index);

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);

        if can_vote && log_is_up_to_date {
            self.voted_for = Some(args.candidate_id);
            reply.vote_granted = true;

            // 重置选举计时器
            self._reset_election_timer();
        }
        reply
    }

    fn _on_append_log(&mut self, log: Vec<u8>) {
        // 检查角色
        if self.role != Role::Leader {
            return;
        }

        // 新增日志
        self.log.push(LogEntry {
            term: self.current_term,
            command: log,
        });

        // 更新提交索引
        self.commit_index += 1;

        // 广播AppendEntries
        self._boardcast_append_entries();
    }

    fn _become_candidate(&mut self) {
        // 更新状态
        self.election_timer.reset();
        self.role = Role::Candidate;
        self.current_term += 1;

        let cur_term = self.current_term;

        let majority = self.members.len() / 2 + 1;

        // 投给自己
        self.voted_for = Some(self.node_id);

        let args = RequestVoteArgs {
            term: cur_term,
            candidate_id: self.node_id,
            last_log_index: self.log.len() as u64 - 1,
            last_log_term: self.log[self.log.len() - 1].term,
        };
        // 发起投票
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

    fn _become_follower(&mut self, term: u64) {
        if term > self.current_term {
            self._refresh_term(term);
        }

        self.role = Role::Follower;
    }

    fn _become_leader(&mut self) {
        // 状态检查
        if self.role == Role::Leader || self.role == Role::Follower {
            return;
        }

        // 更新状态
        self.role = Role::Leader;
        self.next_index
            .iter_mut()
            .for_each(|x| *x.1 = self.log.len() as u64);
        self.match_index.iter_mut().for_each(|x| *x.1 = 0);
    }

    fn _refresh_term(&mut self, new_term: u64) {
        self.current_term = new_term;
        self.role = Role::Follower;
        self.voted_for = None;
    }

    fn _on_heartbeat(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        self._boardcast_append_entries();
    }

    async fn _on_append_entries_reply(&mut self, reply: AppendEntriesReply) {
        if reply.term < self.current_term {
            return;
        }
        if reply.term > self.current_term {
            self._refresh_term(reply.term);
            return;
        }
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }

        if reply.success {
            *self.match_index.get_mut(&reply.node_id).unwrap() = self.log.len() as u64;
            *self.next_index.get_mut(&reply.node_id).unwrap() = self.log.len() as u64;
        } else {
            *self.next_index.get_mut(&reply.node_id).unwrap() -= 1;
        }
    }

    fn _reset_election_timer(&mut self) {
        self.election_timer.reset();
    }

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

impl Rpc for Ruft {
    async fn append_entries(
        self,
        _ctx: context::Context,
        args: AppendEntriesArgs,
    ) -> AppendEntriesReply {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.event_sender.send(Event::AppendEntries(args, tx)).await {
            error!("Error sending AppendEntries event: {err}");
            return AppendEntriesReply {
                node_id: self.node_id,
                term: self.current_term,
                success: false,
            };
        }
        match rx.await {
            Ok(reply) => reply,
            Err(err) => {
                error!("Error receiving AppendEntries reply: {err}");
                AppendEntriesReply {
                    node_id: self.node_id,
                    term: self.current_term,
                    success: false,
                }
            }
        }
    }
    async fn request_vote(self, _ctx: context::Context, args: RequestVoteArgs) -> RequestVoteReply {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.event_sender.send(Event::RequestVote(args, tx)).await {
            error!("Error sending RequestVote event: {err}");
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }
        match rx.await {
            Ok(reply) => reply,
            Err(err) => {
                error!("Error receiving RequestVote reply: {err}");
                RequestVoteReply {
                    term: self.current_term,
                    vote_granted: false,
                }
            }
        }
    }
}
