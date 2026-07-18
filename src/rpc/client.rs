use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use std::sync::Arc;
use tarpc::client::RpcError;

use crate::rpc::{
    append_entries::{AppendEntriesArgs, AppendEntriesReply},
    install_snapshot::{InstallSnapshotArgs, InstallSnapshotReply},
    request_vote::{RequestVoteArgs, RequestVoteReply},
    rpc::RpcClient,
};

// 集群节点 ID。
pub type NodeId = u64;

/// 决定一条出站 RPC 链路是否可用。
/// 返回的许可会在整次 RPC 期间保持存活，供测试传输层跟踪在途请求。
pub type RpcLinkPermit = Box<dyn Send>;
pub type RpcLinkPolicy = Arc<dyn Fn(NodeId, NodeId) -> Option<RpcLinkPermit> + Send + Sync>;

// RPC 客户端集合：保存本节点 ID 和到其他节点的 tarpc client。
#[derive(Clone)]
pub struct RuftClient {
    node_id: NodeId,
    conn: HashMap<NodeId, RpcClient>,
    link_policy: RpcLinkPolicy,
}

impl RuftClient {
    // 构造客户端集合，conn 通常包含集群中可访问的节点连接。
    pub fn new(node_id: NodeId, conn: HashMap<NodeId, RpcClient>) -> Self {
        Self {
            node_id,
            conn,
            link_policy: Arc::new(|_, _| Some(Box::new(()))),
        }
    }

    /// 构造带有出站链路策略的客户端集合。
    pub fn with_link_policy(
        node_id: NodeId,
        conn: HashMap<NodeId, RpcClient>,
        link_policy: RpcLinkPolicy,
    ) -> Self {
        Self {
            node_id,
            conn,
            link_policy,
        }
    }

    // 向指定节点发送 AppendEntries，用于心跳或日志复制。
    pub fn call_append_entries(
        &self,
        node_id: NodeId,
        ctx: tarpc::context::Context,
        args: AppendEntriesArgs,
    ) -> impl Future<Output = Result<AppendEntriesReply, RpcError>> + Send + 'static {
        let link_policy = self.link_policy.clone();
        let source_id = self.node_id;
        let client = self.conn.get(&node_id).cloned();
        async move {
            let _permit = link_policy(source_id, node_id).ok_or(RpcError::Shutdown)?;
            let Some(client) = client else {
                return Err(RpcError::Shutdown);
            };
            client.append_entries(ctx, args).await
        }
    }

    // 向指定节点发送 InstallSnapshot，用于修复落后于快照边界的副本。
    pub fn call_install_snapshot(
        &self,
        node_id: NodeId,
        ctx: tarpc::context::Context,
        args: InstallSnapshotArgs,
    ) -> impl Future<Output = Result<InstallSnapshotReply, RpcError>> + Send + 'static {
        let link_policy = self.link_policy.clone();
        let source_id = self.node_id;
        let client = self.conn.get(&node_id).cloned();
        async move {
            let _permit = link_policy(source_id, node_id).ok_or(RpcError::Shutdown)?;
            let Some(client) = client else {
                return Err(RpcError::Shutdown);
            };
            client.install_snapshot(ctx, args).await
        }
    }

    // 向除自己外的所有节点并发发送 RequestVote。
    pub fn broadcast_request_vote(
        &self,
        ctx: tarpc::context::Context,
        args: RequestVoteArgs,
    ) -> FuturesUnordered<
        impl Future<Output = (u64, Result<RequestVoteReply, RpcError>)> + Send + 'static,
    > {
        let mut futures = FuturesUnordered::new();
        let self_id = self.node_id;
        let link_policy = self.link_policy.clone();
        futures.extend(
            self.conn
                .iter()
                .filter(|(id, _)| **id != self_id)
                .map(|(id, client)| {
                    let id = *id;
                    let client = client.clone();
                    let ctx = ctx.clone();
                    let args = args.clone();
                    let link_policy = link_policy.clone();

                    async move {
                        let Some(_permit) = link_policy(self_id, id) else {
                            return (id, Err(RpcError::Shutdown));
                        };
                        (id, client.request_vote(ctx, args).await)
                    }
                }),
        );

        futures
    }

    // 返回连接数量，通常用于判断集群规模。
    pub fn len(&self) -> usize {
        self.conn.len()
    }
}
