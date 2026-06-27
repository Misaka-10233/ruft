use crate::events::event::Event;
use crate::rpc::append_entries::{AppendEntriesArgs, AppendEntriesReply};
use crate::rpc::client::NodeId;
use crate::rpc::install_snapshot::{InstallSnapshotArgs, InstallSnapshotReply};
use crate::rpc::request_vote::{RequestVoteArgs, RequestVoteReply};
use crate::rpc::rpc::Rpc;
use log::error;
use tarpc::context;
use tokio::sync::{mpsc, oneshot};

// Raft 的网络服务句柄：只负责接收 RPC 并转发到节点事件循环。
// Network service handle for Raft: only receives RPCs and forwards them to the node event loop.
#[derive(Clone)]
pub struct RuftServer {
    // 当前服务所属节点 ID，用于构造错误响应。
    // Node ID for this service, used to build error replies.
    node_id: NodeId,
    // 内部事件发送端，真正的 Raft 状态仍由 Ruft::run 串行处理。
    // Internal event sender; real Raft state is still serialized by Ruft::run.
    event_sender: mpsc::Sender<Event>,
}

impl RuftServer {
    // 创建一个可 clone 的 RPC 服务句柄，供 tarpc server 持有。
    // Creates a cloneable RPC service handle for tarpc servers.
    pub(crate) fn new(node_id: NodeId, event_sender: mpsc::Sender<Event>) -> Self {
        Self {
            node_id,
            event_sender,
        }
    }

    // AppendEntries 出错时没有读取节点状态，统一用 term=0 表示本地转发失败。
    // On AppendEntries forwarding errors, term=0 represents a local forwarding failure.
    fn append_entries_error(&self) -> AppendEntriesReply {
        AppendEntriesReply {
            node_id: self.node_id,
            term: 0,
            success: false,
            match_index: 0,
        }
    }

    // RequestVote 出错时没有读取节点状态，统一用 term=0 表示本地转发失败。
    // On RequestVote forwarding errors, term=0 represents a local forwarding failure.
    fn request_vote_error(&self) -> RequestVoteReply {
        RequestVoteReply {
            term: 0,
            vote_granted: false,
        }
    }

    // InstallSnapshot 出错时没有读取节点状态，统一用 term=0 表示本地转发失败。
    // On InstallSnapshot forwarding errors, term=0 represents a local forwarding failure.
    fn install_snapshot_error(&self) -> InstallSnapshotReply {
        InstallSnapshotReply {
            node_id: self.node_id,
            term: 0,
        }
    }
}

impl Rpc for RuftServer {
    // tarpc 入口只负责把请求转入内部事件循环，并等待一次性响应。
    // tarpc entrypoint only forwards the request into the event loop and waits once.
    async fn append_entries(
        self,
        _ctx: context::Context,
        args: AppendEntriesArgs,
    ) -> AppendEntriesReply {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.event_sender.send(Event::AppendEntries(args, tx)).await {
            error!("Error sending AppendEntries event: {err}");
            return self.append_entries_error();
        }
        match rx.await {
            Ok(reply) => reply,
            Err(err) => {
                error!("Error receiving AppendEntries reply: {err}");
                self.append_entries_error()
            }
        }
    }

    // tarpc 投票入口，同样通过事件循环串行处理，避免和本地状态并发冲突。
    // tarpc vote entrypoint also serializes through the event loop to avoid state races.
    async fn request_vote(self, _ctx: context::Context, args: RequestVoteArgs) -> RequestVoteReply {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.event_sender.send(Event::RequestVote(args, tx)).await {
            error!("Error sending RequestVote event: {err}");
            return self.request_vote_error();
        }
        match rx.await {
            Ok(reply) => reply,
            Err(err) => {
                error!("Error receiving RequestVote reply: {err}");
                self.request_vote_error()
            }
        }
    }

    // tarpc 快照安装入口，通过事件循环串行处理。
    // tarpc snapshot entrypoint, serialized through the event loop.
    async fn install_snapshot(
        self,
        _ctx: context::Context,
        args: InstallSnapshotArgs,
    ) -> InstallSnapshotReply {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.event_sender.send(Event::InstallSnapshot(args, tx)).await {
            error!("Error sending InstallSnapshot event: {err}");
            return self.install_snapshot_error();
        }
        match rx.await {
            Ok(reply) => reply,
            Err(err) => {
                error!("Error receiving InstallSnapshot reply: {err}");
                self.install_snapshot_error()
            }
        }
    }
}
