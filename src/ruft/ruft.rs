use std::cmp::min;
use tokio::sync::oneshot;

use crate::events::event::Event;
use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};
use crate::rpc::rpc::Rpc;
use crate::ruft::role::Role;
use crate::utilis::types::LogEntry;
use tarpc::context;

use tokio::sync::mpsc;

use crate::utilis::timer::Timer;

pub struct Ruft {
    // 节点角色
    role: Role,

    // 持久化通用状态，RPC响应前持久化
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,

    // 临时通用状态
    commit_index: u64,
    last_applied: u64,

    // 作为Leader的临时状态，每次被选举为Leader后重新初始化
    next_index: Vec<u64>,
    match_index: Vec<u64>,

    // 事件管道
    event_productor: mpsc::Sender<Event>,
    event_consumer: mpsc::Receiver<Event>,

    // 选举计时器
    election_timer: Timer,
}

impl Ruft {
    fn handle_event(&mut self, event: Event) {
        match event {
            Event::AppendEntries(_args, _reply) => {
                let reply = self.handle_append_entries(_args);
                match _reply.send(reply) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("Error sending AppendEntries reply: {:?}", err);
                    }
                }
            }
            Event::RequestVote(_args, _reply) => {
                let reply = self._request_vote(_args);
                match _reply.send(reply) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("Error sending RequestVote reply: {:?}", err);
                    }
                }
            }
        }
    }

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        // 重置选举计时器
        self._reset_election_timer();

        let mut reply = AppendEntriesReply {
            term: self.current_term,
            success: false,
        };

        if args.term < self.current_term {
            return reply; // 过时消息
        }

        if args.term > self.current_term {
            // 自身任期过期，更新任期并确认为Follower
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }

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

    fn _request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
        };
        if args.term < self.current_term {
            return reply; // 过时消息
        }
        if (self.voted_for == None || self.voted_for == Some(args.candidate_id))
            && args.last_log_index >= (self.log.len() - 1) as u64
        {
            // 当前任期内未投票且候选人的日志至少与自身一样新
            reply.vote_granted = true;
        }
        reply
    }

    fn _reset_election_timer(&mut self) {
        self.election_timer.reset();
    }
}

impl Rpc for Ruft {
    async fn append_entries(
        self,
        _ctx: context::Context,
        args: AppendEntriesArgs,
    ) -> AppendEntriesReply {
        let (tx, rx) = oneshot::channel();
        match self
            .event_productor
            .send(Event::AppendEntries(args, tx))
            .await
        {
            Ok(_) => {}
            Err(err) => {
                eprintln!("Error sending AppendEntries event: {:?}", err);
            }
        };
        match rx.await {
            Ok(reply) => reply,
            Err(err) => {
                eprintln!("Error receiving AppendEntries reply: {:?}", err);
                AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                }
            }
        }
    }
    async fn request_vote(self, _ctx: context::Context, args: RequestVoteArgs) -> RequestVoteReply {
        let (tx, rx) = oneshot::channel();
        match self
            .event_productor
            .send(Event::RequestVote(args, tx))
            .await
        {
            Ok(_) => {}
            Err(err) => {
                eprintln!("Error sending RequestVote event: {:?}", err);
            }
        };
        match rx.await {
            Ok(reply) => reply,
            Err(err) => {
                eprintln!("Error receiving RequestVote reply: {:?}", err);
                RequestVoteReply {
                    term: self.current_term,
                    vote_granted: false,
                }
            }
        }
    }
}
