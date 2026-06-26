use futures::{StreamExt, future};
use ruft::rpc::client::{NodeId, RuftClient};
use ruft::rpc::rpc::{Rpc, RpcClient};
use ruft::ruft::{Role, Ruft, RuftHandle};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use tarpc::client;
use tarpc::serde_transport::tcp;
use tarpc::server::{BaseChannel, incoming::Incoming};
use tarpc::tokio_serde::formats::Bincode;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep, timeout};

struct TestNode {
    id: NodeId,
    handle: RuftHandle,
    apply: mpsc::UnboundedReceiver<Vec<u8>>,
    tasks: Vec<JoinHandle<()>>,
}

async fn spawn_requests(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

async fn start_cluster() -> Vec<TestNode> {
    start_cluster_with(vec![1, 2, 3], false).await
}

async fn start_cluster_with(members: Vec<NodeId>, persistent: bool) -> Vec<TestNode> {
    let mut listeners = Vec::new();
    let mut addrs = HashMap::<NodeId, SocketAddr>::new();

    for id in members.iter().copied() {
        let listener = tcp::listen("localhost:0", Bincode::default)
            .await
            .expect("bind tarpc listener");
        addrs.insert(id, listener.local_addr());
        listeners.push((id, listener));
    }

    let mut nodes = Vec::new();

    for (id, listener) in listeners {
        let mut clients = HashMap::new();
        for peer_id in members.iter().copied().filter(|peer_id| *peer_id != id) {
            let transport = tcp::connect(addrs[&peer_id], Bincode::default)
                .await
                .expect("connect tarpc peer");
            let client = RpcClient::new(client::Config::default(), transport).spawn();
            clients.insert(peer_id, client);
        }

        let mut ruft = Ruft::new(
            id,
            RuftClient::new(id, clients),
            members.clone(),
            persistent,
            80,
            180,
        )
        .expect("load memory-backed node");
        let server = ruft.server();
        let handle = ruft.handle();
        let apply = ruft.take_apply();

        let server_task = tokio::spawn(
            listener
                .filter_map(|transport| future::ready(transport.ok()))
                .map(BaseChannel::with_defaults)
                .execute(server.serve())
                .map(|channel| channel.for_each(spawn_requests))
                .for_each(spawn_requests),
        );

        let run_task = tokio::spawn(async move {
            ruft.run().await;
        });

        nodes.push(TestNode {
            id,
            handle,
            apply,
            tasks: vec![server_task, run_task],
        });
    }

    nodes
}

async fn wait_for_leader(nodes: &[TestNode]) -> NodeId {
    let active_ids = nodes.iter().map(|node| node.id).collect::<Vec<_>>();
    wait_for_leader_among(nodes, &active_ids).await
}

async fn wait_for_leader_among(nodes: &[TestNode], active_ids: &[NodeId]) -> NodeId {
    let deadline = Instant::now() + Duration::from_secs(8);

    loop {
        let mut leaders = Vec::new();
        for node in nodes.iter().filter(|node| active_ids.contains(&node.id)) {
            let snapshot = node.handle.snapshot().await.expect("node snapshot");
            if snapshot.role == Role::Leader {
                leaders.push(snapshot.node_id);
            }
        }

        leaders.sort_unstable();
        leaders.dedup();
        if leaders.len() == 1 {
            return leaders[0];
        }

        assert!(
            Instant::now() < deadline,
            "expected exactly one leader among {active_ids:?}, got {leaders:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_commit(nodes: &[TestNode], index: u64) {
    let active_ids = nodes.iter().map(|node| node.id).collect::<Vec<_>>();
    wait_for_commit_among(nodes, &active_ids, index).await;
}

async fn wait_for_commit_among(nodes: &[TestNode], active_ids: &[NodeId], index: u64) {
    let deadline = Instant::now() + Duration::from_secs(8);

    loop {
        let mut committed = 0;
        let mut snapshots = Vec::new();
        for node in nodes.iter().filter(|node| active_ids.contains(&node.id)) {
            let snapshot = node.handle.snapshot().await.expect("node snapshot");
            if snapshot.commit_index >= index && snapshot.log_len > index as usize {
                committed += 1;
            }
            snapshots.push(snapshot);
        }

        if committed == active_ids.len() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "expected all nodes to commit index {index}, got {committed}/{}; snapshots: {snapshots:?}",
            active_ids.len()
        );
        sleep(Duration::from_millis(50)).await;
    }
}

fn node_by_id(nodes: &[TestNode], id: NodeId) -> &TestNode {
    nodes.iter().find(|node| node.id == id).expect("node by id")
}

fn node_mut_by_id(nodes: &mut [TestNode], id: NodeId) -> &mut TestNode {
    nodes
        .iter_mut()
        .find(|node| node.id == id)
        .expect("node by id")
}

fn abort_all(nodes: &mut [TestNode]) {
    for node in nodes {
        abort_node(node);
    }
}

fn abort_node(node: &mut TestNode) {
    for task in node.tasks.drain(..) {
        task.abort();
    }
}

async fn recv_applied(node: &mut TestNode) -> Vec<u8> {
    timeout(Duration::from_secs(3), node.apply.recv())
        .await
        .expect("apply timeout")
        .expect("apply channel open")
}

fn default_storage_dir(node_id: NodeId) -> PathBuf {
    PathBuf::from("ruft-data").join(format!("node-{node_id}"))
}

fn clean_storage_dirs(node_ids: &[NodeId]) {
    for node_id in node_ids {
        let _ = fs::remove_dir_all(default_storage_dir(*node_id));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_tcp_nodes_elect_one_leader_and_replicate_log() {
    let mut nodes = start_cluster().await;

    let leader_id = wait_for_leader(&nodes).await;
    let leader = nodes
        .iter()
        .find(|node| node.id == leader_id)
        .expect("leader node");

    assert!(
        leader
            .handle
            .append_log(b"set x=1".to_vec())
            .await
            .expect("append reply"),
        "leader should accept client log"
    );

    wait_for_commit(&nodes, 1).await;

    for node in nodes.iter_mut() {
        let applied = recv_applied(node).await;
        assert_eq!(applied, b"set x=1".to_vec(), "node {} applied log", node.id);
    }

    abort_all(&mut nodes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_rejects_client_append() {
    let mut nodes = start_cluster().await;

    let leader_id = wait_for_leader(&nodes).await;
    let follower = nodes
        .iter()
        .find(|node| node.id != leader_id)
        .expect("follower node");

    assert!(
        !follower
            .handle
            .append_log(b"should not append".to_vec())
            .await
            .expect("append reply"),
        "follower should reject client log"
    );

    sleep(Duration::from_millis(200)).await;
    for node in &nodes {
        let snapshot = node.handle.snapshot().await.expect("node snapshot");
        assert_eq!(snapshot.commit_index, 0, "node {} commit index", node.id);
        assert_eq!(snapshot.log_len, 1, "node {} log length", node.id);
    }

    abort_all(&mut nodes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_replicates_multiple_logs_in_order() {
    let mut nodes = start_cluster().await;

    let leader_id = wait_for_leader(&nodes).await;
    let leader = node_by_id(&nodes, leader_id);
    let commands = vec![b"set x=1".to_vec(), b"set y=2".to_vec(), b"commit".to_vec()];

    for command in &commands {
        assert!(
            leader
                .handle
                .append_log(command.clone())
                .await
                .expect("append reply"),
            "leader should accept client log"
        );
    }

    wait_for_commit(&nodes, commands.len() as u64).await;

    for node in nodes.iter_mut() {
        let mut applied = Vec::new();
        for _ in 0..commands.len() {
            applied.push(recv_applied(node).await);
        }
        assert_eq!(applied, commands, "node {} applied log order", node.id);
    }

    abort_all(&mut nodes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_elects_new_leader_after_current_leader_stops() {
    let mut nodes = start_cluster().await;

    let old_leader_id = wait_for_leader(&nodes).await;
    abort_node(node_mut_by_id(&mut nodes, old_leader_id));

    let active_ids = nodes
        .iter()
        .map(|node| node.id)
        .filter(|id| *id != old_leader_id)
        .collect::<Vec<_>>();
    let new_leader_id = wait_for_leader_among(&nodes, &active_ids).await;
    let new_leader = node_by_id(&nodes, new_leader_id);

    assert!(
        new_leader
            .handle
            .append_log(b"after failover".to_vec())
            .await
            .expect("append reply"),
        "new leader should accept client log"
    );

    wait_for_commit_among(&nodes, &active_ids, 1).await;

    for node_id in active_ids {
        let node = node_mut_by_id(&mut nodes, node_id);
        let applied = recv_applied(node).await;
        assert_eq!(
            applied,
            b"after failover".to_vec(),
            "node {} applied failover log",
            node.id
        );
    }

    abort_all(&mut nodes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_node_restores_state_after_restart() {
    let node_ids = vec![20_001, 20_002, 20_003];
    clean_storage_dirs(&node_ids);

    let mut nodes = start_cluster_with(node_ids.clone(), true).await;
    let leader_id = wait_for_leader(&nodes).await;
    let leader = node_by_id(&nodes, leader_id);

    assert!(
        leader
            .handle
            .append_log(b"durable entry".to_vec())
            .await
            .expect("append reply"),
        "leader should accept durable log"
    );
    wait_for_commit(&nodes, 1).await;

    let leader_snapshot = leader.handle.snapshot().await.expect("leader snapshot");
    abort_all(&mut nodes);
    sleep(Duration::from_millis(100)).await;

    let restored = Ruft::new(
        leader_id,
        RuftClient::new(leader_id, HashMap::new()),
        node_ids.clone(),
        true,
        80,
        180,
    )
    .expect("restore persistent node");
    let restored_snapshot = restored.snapshot();

    assert_eq!(restored_snapshot.current_term, leader_snapshot.current_term);
    assert_eq!(restored_snapshot.log_len, 2);

    clean_storage_dirs(&node_ids);
}
