use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use tarpc::client::RpcError;

use crate::rpc::{
    append_entries::{AppendEntriesArgs, AppendEntriesReply},
    request_vote::{RequestVoteArgs, RequestVoteReply},
    rpc::RpcClient,
};

// 集群节点 ID。Cluster node ID.
pub type NodeId = u64;

// RPC 客户端集合：保存本节点 ID 和到其他节点的 tarpc client。
// RPC client set: keeps this node ID and tarpc clients to peers.
pub struct RuftClient {
    node_id: NodeId,
    conn: HashMap<NodeId, RpcClient>,
}

impl RuftClient {
    // 构造客户端集合，conn 通常包含集群中可访问的节点连接。
    // Builds the client set; conn usually contains reachable cluster clients.
    pub fn new(node_id: NodeId, conn: HashMap<NodeId, RpcClient>) -> Self {
        Self {
            node_id,
            conn: conn,
        }
    }

    // 向指定节点发送 AppendEntries，用于心跳或日志复制。
    // Sends AppendEntries to one peer for heartbeat or log replication.
    pub fn call_append_entries(
        &self,
        node_id: NodeId,
        ctx: tarpc::context::Context,
        args: AppendEntriesArgs,
    ) -> impl Future<Output = Result<AppendEntriesReply, RpcError>> + '_ {
        self.conn[&node_id].append_entries(ctx, args)
    }

    // 向除自己外的所有节点并发发送 RequestVote。
    // Broadcasts RequestVote concurrently to every peer except self.
    pub fn broadcast_request_vote(
        &self,
        ctx: tarpc::context::Context,
        args: RequestVoteArgs,
    ) -> FuturesUnordered<
        impl Future<Output = (u64, Result<RequestVoteReply, RpcError>)> + Send + 'static,
    > {
        let mut futures = FuturesUnordered::new();
        let self_id = self.node_id;
        futures.extend(
            self.conn
                .iter()
                .filter(|(id, _)| **id != self_id)
                .map(|(id, client)| {
                    let id = *id;
                    let client = client.clone();
                    let ctx = ctx.clone();
                    let args = args.clone();

                    async move { (id, client.request_vote(ctx, args).await) }
                }),
        );

        futures
    }

    // 返回连接数量，通常用于判断集群规模。
    // Returns connection count, usually to reason about cluster size.
    pub fn len(&self) -> usize {
        self.conn.len()
    }
}
