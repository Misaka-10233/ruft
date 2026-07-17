use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::{StreamExt, future};
use log::{error, warn};

use ruft::{
    result::AppendResult,
    rpc::client::{NodeId, RuftClient},
    rpc::rpc::Rpc,
    ruft::{ApplyMsg, Ruft},
    storage::{FileStorage, Storage},
};
use std::{
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tarpc::serde_transport::tcp;
use tarpc::server::{BaseChannel, incoming::Incoming};
use tarpc::tokio_serde::formats::Bincode;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct GetArgs {
    key: String,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct GetReply {
    exist: bool,
    value: String,
    term: u64,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SetArgs {
    key: String,
    new_value: String,
    new_term: u64,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SetReply {
    success: bool,
    cur_value: String,
    cur_term: u64,
}

#[tarpc::service]
trait KVStore {
    async fn get(args: GetArgs) -> GetReply;
    async fn set(args: SetArgs) -> SetReply;
}

struct Record {
    value: String,
    term: u64,
}

type Store = Arc<Mutex<std::collections::HashMap<String, Record>>>;
type RequestId = u64;
type PendingRequests =
    Arc<Mutex<std::collections::HashMap<RequestId, oneshot::Sender<ApplyResult>>>>;

// The request ID travels in the replicated command, so an applied command can
// complete the exact RPC that submitted it without depending on Raft log index timing.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Command {
    request_id: RequestId,
    operation: Operation,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum Operation {
    Get {
        key: String,
    },
    Set {
        key: String,
        new_value: String,
        new_term: u64,
    },
}

enum ApplyResult {
    Get(GetReply),
    Set(SetReply),
}

struct RuftKVServer {
    store: Store,

    ruft: Ruft,

    pending_requests: PendingRequests,
    next_request_id: Arc<AtomicU64>,
}

#[derive(Clone)]
struct KVStoreService {
    ruft: ruft::ruft::RuftHandle,
    pending_requests: PendingRequests,
    next_request_id: Arc<AtomicU64>,
}

type Conn = std::collections::HashMap<u64, ruft::rpc::rpc::RpcClient>;

async fn spawn_request(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

impl RuftKVServer {
    fn new(node_id: NodeId, conn: Conn, persistent: bool, storage_root: PathBuf) -> Self {
        let mut mem = conn.iter().map(|(k, _)| *k).collect::<Vec<_>>();
        mem.push(node_id);
        let ruft_client = RuftClient::new(node_id, conn);
        let r = Ruft::new(
            node_id,
            ruft_client,
            mem,
            persistent,
            storage_root.clone(),
            200,
            500,
            1024,
            1024,
        )
        .expect("open Raft storage");
        let (store, next_request_id) = if persistent {
            Self::recover_store(&storage_root, node_id).expect("recover KV state from Raft log")
        } else {
            (Arc::new(Mutex::new(std::collections::HashMap::new())), 1)
        };
        Self {
            store,
            ruft: r,
            pending_requests: Arc::new(Mutex::new(std::collections::HashMap::new())),
            next_request_id: Arc::new(AtomicU64::new(next_request_id)),
        }
    }

    fn recover_store(
        storage_root: &std::path::Path,
        node_id: NodeId,
    ) -> Result<(Store, RequestId), String> {
        let storage = FileStorage::open(storage_root, node_id)
            .map_err(|err| format!("open node storage: {err}"))?;
        let state = storage
            .load()
            .map_err(|err| format!("load node storage: {err}"))?;
        if state.snapshot.is_some() {
            return Err(
                "KV benchmark cannot restore a Raft snapshot because it does not store KV snapshot data"
                    .to_string(),
            );
        }

        let mut recovered = std::collections::HashMap::new();
        let mut max_request_id = 0;
        for (offset, entry) in state.log.iter().enumerate().skip(1) {
            let command: Command = bincode::deserialize(&entry.command)
                .map_err(|err| format!("decode recovered command at log index {offset}: {err}"))?;
            max_request_id = max_request_id.max(command.request_id);
            Self::apply_operation(&mut recovered, &command.operation);
        }
        let next_request_id = max_request_id
            .checked_add(1)
            .ok_or_else(|| "recovered request IDs exhausted u64".to_string())?;
        Ok((Arc::new(Mutex::new(recovered)), next_request_id))
    }

    fn apply_operation(
        store: &mut std::collections::HashMap<String, Record>,
        operation: &Operation,
    ) -> ApplyResult {
        match operation {
            Operation::Set {
                key,
                new_value,
                new_term,
            } => {
                let current = store
                    .get(key)
                    .map(|record| (record.value.clone(), record.term));
                let reply = match current {
                    Some((_, cur_term)) if *new_term == cur_term + 1 => {
                        store.insert(
                            key.clone(),
                            Record {
                                value: new_value.clone(),
                                term: *new_term,
                            },
                        );
                        SetReply {
                            success: true,
                            cur_value: new_value.clone(),
                            cur_term: *new_term,
                        }
                    }
                    Some((cur_value, cur_term)) => SetReply {
                        success: false,
                        cur_value,
                        cur_term,
                    },
                    None if *new_term == 1 => {
                        store.insert(
                            key.clone(),
                            Record {
                                value: new_value.clone(),
                                term: *new_term,
                            },
                        );
                        SetReply {
                            success: true,
                            cur_value: new_value.clone(),
                            cur_term: *new_term,
                        }
                    }
                    None => SetReply {
                        success: false,
                        cur_value: String::new(),
                        cur_term: 0,
                    },
                };
                ApplyResult::Set(reply)
            }
            Operation::Get { key } => {
                let (exist, value, term) = match store.get(key) {
                    Some(record) => (true, record.value.clone(), record.term),
                    None => (false, String::new(), 0),
                };
                ApplyResult::Get(GetReply { exist, value, term })
            }
        }
    }

    async fn serve(self, raft_tcp_listener: TcpListener, kv_tcp_listener: TcpListener) {
        let raft_listener = tcp::listen_on(raft_tcp_listener, Bincode::default)
            .await
            .expect("create Raft tarpc listener");
        let kv_listener = tcp::listen_on(kv_tcp_listener, Bincode::default)
            .await
            .expect("create KVStore tarpc listener");
        let RuftKVServer {
            store,
            mut ruft,
            pending_requests,
            next_request_id,
        } = self;
        let mut receiver = ruft
            .take_applied_receiver()
            .expect("applied receiver has already been taken");
        let raft_service = ruft.server();
        let kv_service = KVStoreService {
            ruft: ruft.handle(),
            pending_requests: pending_requests.clone(),
            next_request_id,
        };

        let mut join_handles = Vec::new();

        join_handles.push(tokio::spawn(
            raft_listener
                .filter_map(|transport| future::ready(transport.ok()))
                .map(BaseChannel::with_defaults)
                .execute(raft_service.serve())
                .map(|channel| channel.for_each(spawn_request))
                .for_each(spawn_request),
        ));

        join_handles.push(tokio::spawn(
            kv_listener
                .filter_map(|transport| future::ready(transport.ok()))
                .map(BaseChannel::with_defaults)
                .execute(kv_service.serve())
                .map(|channel| channel.for_each(spawn_request))
                .for_each(spawn_request),
        ));

        join_handles.push(tokio::spawn(async move {
            ruft.run().await;
        }));

        join_handles.push(tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    None => {
                        error!("Applied receiver closed");
                        break;
                    }
                    Some(msg) => match msg {
                        ApplyMsg::Command { index, data } => {
                            Self::handle_log_commit(&store, &pending_requests, index, data).await;
                        }
                        ApplyMsg::Snapshot { index, term, data } => {
                            warn!(
                                "Snapshot applied at index {index}, term {term}, data length: {}",
                                data.len()
                            );
                        }
                    },
                }
            }
        }));

        for handle in join_handles {
            let _ = handle.await.expect("thread join failed");
        }
    }

    async fn handle_log_commit(
        store: &Store,
        pending_requests: &PendingRequests,
        index: u64,
        log_entry: Vec<u8>,
    ) {
        let command: Command = match bincode::deserialize(&log_entry) {
            Ok(command) => command,
            Err(err) => {
                error!("Invalid command at index {index}: {err}");
                return;
            }
        };

        let mut store = store.lock().await;
        let result = Self::apply_operation(&mut store, &command.operation);

        // The waiter is installed before append_log, so a fast commit cannot lose this reply.
        if let Some(tx) = pending_requests.lock().await.remove(&command.request_id) {
            let _ = tx.send(result);
        }
    }
}

struct KvCluster {
    client_groups: Vec<Vec<KVStoreClient>>,
    leader_index: Arc<AtomicU64>,
    next_key: AtomicU64,
    run_id: u128,
    tasks: Vec<JoinHandle<()>>,
}

impl KvCluster {
    async fn start(persistent: bool, client_count: usize) -> Self {
        let node_ids = [1, 2, 3];
        let mut raft_listeners = Vec::new();
        let mut kv_listeners = Vec::new();
        let mut raft_addresses = std::collections::HashMap::new();
        let mut kv_addresses = Vec::new();

        for node_id in node_ids {
            let raft_listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind Raft TCP listener");
            raft_addresses.insert(node_id, raft_listener.local_addr().expect("Raft address"));
            raft_listeners.push((node_id, raft_listener));

            let kv_listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind KV TCP listener");
            kv_addresses.push(kv_listener.local_addr().expect("KV address"));
            kv_listeners.push(kv_listener);
        }

        let mut tasks = Vec::new();
        for ((node_id, raft_listener), kv_listener) in raft_listeners.into_iter().zip(kv_listeners)
        {
            let mut connections = Conn::new();
            for peer_id in node_ids.into_iter().filter(|peer_id| *peer_id != node_id) {
                let transport = tcp::connect(raft_addresses[&peer_id], Bincode::default)
                    .await
                    .expect("connect Raft peer");
                let client =
                    ruft::rpc::rpc::RpcClient::new(tarpc::client::Config::default(), transport)
                        .spawn();
                connections.insert(peer_id, client);
            }

            let server = RuftKVServer::new(
                node_id,
                connections,
                persistent,
                PathBuf::from("target/kvserver-benchmark-data"),
            );
            tasks.push(tokio::spawn(server.serve(raft_listener, kv_listener)));
        }

        let mut client_groups = Vec::with_capacity(client_count);
        for _ in 0..client_count {
            let mut clients = Vec::with_capacity(kv_addresses.len());
            for address in &kv_addresses {
                clients.push(
                    KVStoreClient::new(
                        tarpc::client::Config::default(),
                        tcp::connect(*address, Bincode::default)
                            .await
                            .expect("connect KV client"),
                    )
                    .spawn(),
                );
            }
            client_groups.push(clients);
        }

        let cluster = Self {
            client_groups,
            leader_index: Arc::new(AtomicU64::new(0)),
            next_key: AtomicU64::new(0),
            run_id: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before Unix epoch")
                .as_nanos(),
            tasks,
        };
        cluster.wait_for_leader().await;
        cluster
    }

    async fn wait_for_leader(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let key_id = self.next_key.fetch_add(1, Ordering::Relaxed);
            if self.set_on_leader(0, key_id).await.success {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "three-node cluster did not elect a leader"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn set_on_leader(&self, client_index: usize, key_id: u64) -> SetReply {
        let clients = &self.client_groups[client_index];
        let selected = self.leader_index.load(Ordering::Relaxed) as usize;
        for offset in 0..clients.len() {
            let index = (selected + offset) % clients.len();
            let reply = timeout(
                Duration::from_secs(5),
                clients[index].set(
                    tarpc::context::Context::current(),
                    SetArgs {
                        key: format!("benchmark-key-{}-{key_id}", self.run_id),
                        new_value: "value".to_string(),
                        new_term: 1,
                    },
                ),
            )
            .await
            .expect("KV set timed out")
            .expect("KV TCP request failed");
            if reply.success {
                self.leader_index.store(index as u64, Ordering::Relaxed);
                return reply;
            }
        }

        SetReply {
            success: false,
            cur_value: String::new(),
            cur_term: 0,
        }
    }

    async fn set_next_key(&self, client_index: usize) -> SetReply {
        let key_id = self.next_key.fetch_add(1, Ordering::Relaxed);
        self.set_on_leader(client_index, key_id).await
    }
}

impl Drop for KvCluster {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct BenchmarkOptions {
    persistent: bool,
    client_count: usize,
}

impl BenchmarkOptions {
    fn from_args() -> Self {
        let mut persistent = false;
        let mut client_count = 1;

        for argument in std::env::args() {
            if argument == "persistent" {
                persistent = true;
            } else if let Some(value) = argument.strip_prefix("clients=") {
                client_count = Self::parse_client_count(value);
            } else if let Some(value) = argument.strip_prefix("persistent,clients=") {
                persistent = true;
                client_count = Self::parse_client_count(value);
            }
        }

        Self {
            persistent,
            client_count,
        }
    }

    fn parse_client_count(value: &str) -> usize {
        value
            .parse()
            .ok()
            .filter(|count: &usize| *count > 0)
            .expect("clients must be a positive integer")
    }

    fn profile_name(&self) -> String {
        let storage = if self.persistent {
            "persistent"
        } else {
            "memory"
        };
        format!("{storage},clients={}", self.client_count)
    }
}

fn benchmark_kvserver(c: &mut Criterion) {
    let options = BenchmarkOptions::from_args();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create benchmark runtime");
    let cluster = runtime.block_on(KvCluster::start(options.persistent, options.client_count));

    let mut group = c.benchmark_group(format!("three_node_tarpc_tcp/{}", options.profile_name()));
    group.throughput(Throughput::Elements(options.client_count as u64));
    let client_count = options.client_count;
    group.bench_function("kv_set_commit", |bench| {
        bench.to_async(&runtime).iter(|| async {
            let replies = future::join_all(
                (0..client_count).map(|client_index| cluster.set_next_key(client_index)),
            )
            .await;
            for reply in replies {
                assert!(reply.success, "leader must commit the KV write");
                std::hint::black_box(reply);
            }
        });
    });
    group.finish();
}

criterion_group!(benches, benchmark_kvserver);
criterion_main!(benches);

impl KVStore for KVStoreService {
    async fn get(self, _: tarpc::context::Context, args: GetArgs) -> GetReply {
        let err_reply = GetReply {
            exist: false,
            value: "".to_string(),
            term: 0,
        };
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        // Register before appending: ApplyMsg may arrive immediately after the append succeeds.
        self.pending_requests.lock().await.insert(request_id, tx);
        let command = Command {
            request_id,
            operation: Operation::Get { key: args.key },
        };
        let data = bincode::serialize(&command).expect("command serialization must succeed");

        match self.ruft.append_log(data).await {
            Ok(AppendResult::Accepted { .. }) => {}
            Ok(AppendResult::NotLeader | AppendResult::PersistentError) => {
                self.pending_requests.lock().await.remove(&request_id);
                return err_reply;
            }
            Err(err) => {
                self.pending_requests.lock().await.remove(&request_id);
                error!("Error appending log: {err}");
                return err_reply;
            }
        }
        match rx.await {
            Ok(ApplyResult::Get(reply)) => reply,
            Ok(ApplyResult::Set(_)) => {
                error!("GET request {request_id} completed with a SET result");
                err_reply
            }
            Err(err) => {
                error!("Error receiving GetReply: {err}");
                err_reply
            }
        }
    }

    async fn set(self, _: tarpc::context::Context, args: SetArgs) -> SetReply {
        let err_reply = SetReply {
            success: false,
            cur_value: "".to_string(),
            cur_term: 0,
        };
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        // Register before appending: ApplyMsg may arrive immediately after the append succeeds.
        self.pending_requests.lock().await.insert(request_id, tx);
        let command = Command {
            request_id,
            operation: Operation::Set {
                key: args.key,
                new_value: args.new_value,
                new_term: args.new_term,
            },
        };
        let data = bincode::serialize(&command).expect("command serialization must succeed");

        match self.ruft.append_log(data).await {
            Ok(AppendResult::Accepted { .. }) => {}
            Ok(AppendResult::NotLeader | AppendResult::PersistentError) => {
                self.pending_requests.lock().await.remove(&request_id);
                return err_reply;
            }
            Err(err) => {
                self.pending_requests.lock().await.remove(&request_id);
                error!("Error appending log: {err}");
                return err_reply;
            }
        }
        match rx.await {
            Ok(ApplyResult::Set(reply)) => reply,
            Ok(ApplyResult::Get(_)) => {
                error!("SET request {request_id} completed with a GET result");
                err_reply
            }
            Err(err) => {
                error!("Error receiving SetReply: {err}");
                err_reply
            }
        }
    }
}
