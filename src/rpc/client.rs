use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use tarpc::client::RpcError;

use crate::rpc::{
    append_entries::{AppendEntriesArgs, AppendEntriesReply},
    request_vote::{RequestVoteArgs, RequestVoteReply},
    rpc::RpcClient,
};

pub type NodeId = u64;

pub struct RuftClient {
    node_id: NodeId,
    conn: HashMap<NodeId, RpcClient>,
}

impl RuftClient {
    pub fn new(node_id: NodeId, conn: HashMap<NodeId, RpcClient>) -> Self {
        Self {
            node_id,
            conn: conn,
        }
    }

    pub fn call_append_entries(
        &self,
        node_id: NodeId,
        ctx: tarpc::context::Context,
        args: AppendEntriesArgs,
    ) -> impl Future<Output = Result<AppendEntriesReply, RpcError>> + '_ {
        self.conn[&node_id].append_entries(ctx, args)
    }

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

    pub fn len(&self) -> usize {
        self.conn.len()
    }
}
