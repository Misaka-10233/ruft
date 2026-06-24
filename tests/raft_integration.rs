use futures::{StreamExt, future};
use ruft::rpc::client::{NodeId, RuftClient};
use ruft::rpc::rpc::{Rpc, RpcClient};
use ruft::ruft::{Role, Ruft, RuftHandle};
use std::collections::HashMap;
use std::net::SocketAddr;
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
    let members = vec![1, 2, 3];
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

        let mut ruft = Ruft::new(id, RuftClient::new(id, clients), members.clone(), 80, 180);
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
    let deadline = Instant::now() + Duration::from_secs(8);

    loop {
        let mut leaders = Vec::new();
        for node in nodes {
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
            "expected exactly one leader, got {leaders:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_commit(nodes: &[TestNode], index: u64) {
    let deadline = Instant::now() + Duration::from_secs(8);

    loop {
        let mut committed = 0;
        let mut snapshots = Vec::new();
        for node in nodes {
            let snapshot = node.handle.snapshot().await.expect("node snapshot");
            if snapshot.commit_index >= index && snapshot.log_len > index as usize {
                committed += 1;
            }
            snapshots.push(snapshot);
        }

        if committed == nodes.len() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "expected all nodes to commit index {index}, got {committed}/{}; snapshots: {snapshots:?}",
            nodes.len()
        );
        sleep(Duration::from_millis(50)).await;
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
        let applied = timeout(Duration::from_secs(3), node.apply.recv())
            .await
            .expect("apply timeout")
            .expect("apply channel open");
        assert_eq!(applied, b"set x=1".to_vec(), "node {} applied log", node.id);
    }

    for node in nodes {
        for task in node.tasks {
            task.abort();
        }
    }
}
