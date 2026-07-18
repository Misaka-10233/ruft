use futures::{StreamExt, future};
use ruft::rpc::client::{NodeId, RpcLinkPolicy, RuftClient};
use ruft::rpc::rpc::{Rpc, RpcClient};
use ruft::ruft::{ApplyMsg, Role, Ruft, RuftHandle, RuftInfo};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tarpc::client;
use tarpc::serde_transport::tcp;
use tarpc::server::{BaseChannel, incoming::Incoming};
use tarpc::tokio_serde::formats::Bincode;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep, timeout};

pub struct TestNode {
    pub id: NodeId,
    pub handle: RuftHandle,
    apply: mpsc::Receiver<ApplyMsg>,
    tasks: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct PartitionController {
    state: Arc<Mutex<PartitionState>>,
    members: Vec<NodeId>,
    quiescent: Arc<tokio::sync::Notify>,
}

struct PartitionState {
    allowed: HashSet<(NodeId, NodeId)>,
    in_flight: usize,
}

struct InFlightRpc {
    state: Arc<Mutex<PartitionState>>,
    quiescent: Arc<tokio::sync::Notify>,
}

impl Drop for InFlightRpc {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("partition controller lock");
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.quiescent.notify_waiters();
        }
    }
}

impl PartitionController {
    pub fn new(members: Vec<NodeId>) -> Self {
        let allowed = members
            .iter()
            .flat_map(|from| {
                members
                    .iter()
                    .filter(move |to| *from != **to)
                    .map(move |to| (*from, *to))
            })
            .collect();
        Self {
            state: Arc::new(Mutex::new(PartitionState {
                allowed,
                in_flight: 0,
            })),
            members,
            quiescent: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn link_policy(&self) -> RpcLinkPolicy {
        let shared_state = self.state.clone();
        let quiescent = self.quiescent.clone();
        Arc::new(move |from, to| {
            let mut state = shared_state.lock().expect("partition controller lock");
            if !state.allowed.contains(&(from, to)) {
                return None;
            }
            state.in_flight += 1;
            Some(Box::new(InFlightRpc {
                state: shared_state.clone(),
                quiescent: quiescent.clone(),
            }))
        })
    }

    pub fn partition(&self, left: &[NodeId], right: &[NodeId]) {
        let mut state = self.state.lock().expect("partition controller lock");
        for from in left {
            for to in right {
                state.allowed.remove(&(*from, *to));
                state.allowed.remove(&(*to, *from));
            }
        }
    }

    #[allow(dead_code)]
    pub async fn wait_for_quiescence(&self) {
        loop {
            let notified = self.quiescent.notified();
            if self
                .state
                .lock()
                .expect("partition controller lock")
                .in_flight
                == 0
            {
                return;
            }
            notified.await;
        }
    }

    pub fn heal_all(&self) {
        let mut state = self.state.lock().expect("partition controller lock");
        state.allowed.clear();
        for from in &self.members {
            for to in &self.members {
                if from != to {
                    state.allowed.insert((*from, *to));
                }
            }
        }
    }
}

async fn spawn_requests(future: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(future);
}

pub async fn start_cluster(
    members: Vec<NodeId>,
    controller: &PartitionController,
) -> Vec<TestNode> {
    let mut listeners = Vec::new();
    let mut addresses = HashMap::<NodeId, SocketAddr>::new();

    for id in members.iter().copied() {
        let listener = tcp::listen("localhost:0", Bincode::default)
            .await
            .expect("bind tarpc listener");
        addresses.insert(id, listener.local_addr());
        listeners.push((id, listener));
    }

    let mut nodes = Vec::new();
    for (id, listener) in listeners {
        let mut clients = HashMap::new();
        for peer_id in members.iter().copied().filter(|peer_id| *peer_id != id) {
            let transport = tcp::connect(addresses[&peer_id], Bincode::default)
                .await
                .expect("connect tarpc peer");
            clients.insert(
                peer_id,
                RpcClient::new(client::Config::default(), transport).spawn(),
            );
        }

        let mut ruft = Ruft::new(
            id,
            RuftClient::with_link_policy(id, clients, controller.link_policy()),
            members.clone(),
            false,
            PathBuf::from("raft-data"),
            80,
            180,
            1024,
            1024,
        )
        .expect("create in-memory node");
        let server = ruft.server();
        let handle = ruft.handle();
        let apply = ruft
            .take_applied_receiver()
            .expect("take applied receiver once");
        let server_task = tokio::spawn(
            listener
                .filter_map(|transport| future::ready(transport.ok()))
                .map(BaseChannel::with_defaults)
                .execute(server.serve())
                .map(|channel| channel.for_each(spawn_requests))
                .for_each(spawn_requests),
        );
        let run_task = tokio::spawn(async move { ruft.run().await });
        nodes.push(TestNode {
            id,
            handle,
            apply,
            tasks: vec![server_task, run_task],
        });
    }
    nodes
}

pub async fn wait_for_leader_among(nodes: &[TestNode], members: &[NodeId]) -> NodeId {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let mut leaders = Vec::new();
        for node in nodes.iter().filter(|node| members.contains(&node.id)) {
            if node.handle.get_info().await.expect("node info").role == Role::Leader {
                leaders.push(node.id);
            }
        }
        leaders.sort_unstable();
        leaders.dedup();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(
            Instant::now() < deadline,
            "expected one leader among {members:?}, found {leaders:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_for_commit_among(nodes: &[TestNode], members: &[NodeId], index: u64) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let infos = collect_infos(nodes).await;
        let committed = infos
            .iter()
            .filter(|info| {
                members.contains(&info.node_id)
                    && info.commit_index >= index
                    && info.last_applied >= index
            })
            .count();
        if committed == members.len() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {members:?} to commit index {index}; infos: {infos:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[allow(dead_code)]
pub async fn wait_for_snapshot_applied(node: &mut TestNode, index: u64, data: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let message = timeout(remaining, node.apply.recv())
            .await
            .expect("snapshot apply timeout")
            .expect("apply channel closed");
        match message {
            ApplyMsg::Snapshot {
                index: received_index,
                data: received_data,
                ..
            } => {
                assert_eq!(received_index, index, "installed snapshot index");
                assert_eq!(received_data, data, "installed snapshot data");
                return;
            }
            ApplyMsg::Command {
                index: command_index,
                ..
            } if command_index < index => continue,
            message => panic!("expected snapshot at index {index}, got {message:?}"),
        }
    }
}

pub async fn recv_command(node: &mut TestNode) -> (u64, Vec<u8>) {
    match timeout(Duration::from_secs(8), node.apply.recv())
        .await
        .expect("command apply timeout")
        .expect("apply channel closed")
    {
        ApplyMsg::Command { index, data } => (index, data),
        message => panic!("expected command apply message, got {message:?}"),
    }
}

#[allow(dead_code)]
pub async fn assert_no_apply(node: &mut TestNode, duration: Duration) {
    assert!(
        timeout(duration, node.apply.recv()).await.is_err(),
        "node {} unexpectedly applied an entry",
        node.id
    );
}

pub async fn collect_infos(nodes: &[TestNode]) -> Vec<RuftInfo> {
    let mut infos = Vec::with_capacity(nodes.len());
    for node in nodes {
        infos.push(node.handle.get_info().await.expect("node info"));
    }
    infos
}

pub fn node_by_id(nodes: &[TestNode], id: NodeId) -> &TestNode {
    nodes.iter().find(|node| node.id == id).expect("node by id")
}

pub fn node_mut_by_id(nodes: &mut [TestNode], id: NodeId) -> &mut TestNode {
    nodes
        .iter_mut()
        .find(|node| node.id == id)
        .expect("node by id")
}

pub fn abort_all(nodes: &mut [TestNode]) {
    for node in nodes {
        for task in node.tasks.drain(..) {
            task.abort();
        }
    }
}
