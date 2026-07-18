mod common;

use common::{
    PartitionController, abort_all, collect_infos, node_by_id, node_mut_by_id, recv_command,
    start_cluster, wait_for_commit_among, wait_for_leader_among,
};
use ruft::result::AppendResult;
use ruft::ruft::Role;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn majority_partition_elects_new_leader_and_cluster_recovers() {
    let members = vec![33_001, 33_002, 33_003];
    let controller = PartitionController::new(members.clone());
    let mut nodes = start_cluster(members.clone(), &controller).await;

    let old_leader_id = wait_for_leader_among(&nodes, &members).await;
    let old_term = node_by_id(&nodes, old_leader_id)
        .handle
        .get_info()
        .await
        .expect("old leader info")
        .current_term;
    let majority = members
        .iter()
        .copied()
        .filter(|id| *id != old_leader_id)
        .collect::<Vec<_>>();

    let baseline = b"before partition".to_vec();
    assert!(matches!(
        node_by_id(&nodes, old_leader_id)
            .handle
            .append_log(baseline.clone())
            .await
            .expect("append baseline"),
        AppendResult::Accepted { index: 1, term } if term == old_term
    ));
    wait_for_commit_among(&nodes, &members, 1).await;
    for id in &members {
        assert_eq!(
            recv_command(node_mut_by_id(&mut nodes, *id)).await,
            (1, baseline.clone()),
            "node {id} applied baseline"
        );
    }

    controller.partition(&[old_leader_id], &majority);
    let new_leader_id = wait_for_leader_among(&nodes, &majority).await;
    let new_term = node_by_id(&nodes, new_leader_id)
        .handle
        .get_info()
        .await
        .expect("new leader info")
        .current_term;
    assert_ne!(new_leader_id, old_leader_id);
    assert!(
        new_term > old_term,
        "majority leader must use a later term: {new_term} <= {old_term}"
    );

    let majority_command = b"committed by new majority leader".to_vec();
    assert!(matches!(
        node_by_id(&nodes, new_leader_id)
            .handle
            .append_log(majority_command.clone())
            .await
            .expect("append to majority leader"),
        AppendResult::Accepted { index: 2, term } if term == new_term
    ));
    wait_for_commit_among(&nodes, &majority, 2).await;
    for id in &majority {
        assert_eq!(
            recv_command(node_mut_by_id(&mut nodes, *id)).await,
            (2, majority_command.clone()),
            "majority node {id} applied the post-election command"
        );
    }

    controller.heal_all();
    wait_for_commit_among(&nodes, &members, 2).await;
    assert_eq!(
        recv_command(node_mut_by_id(&mut nodes, old_leader_id)).await,
        (2, majority_command),
        "the former leader must catch up after healing"
    );

    let recovered_leader_id = wait_for_leader_among(&nodes, &members).await;
    let infos = collect_infos(&nodes).await;
    assert!(
        infos
            .iter()
            .filter(|info| info.role == Role::Leader)
            .all(|info| info.node_id == recovered_leader_id),
        "the recovered cluster must have one leader: {infos:?}"
    );
    let recovered_term = infos
        .iter()
        .find(|info| info.node_id == recovered_leader_id)
        .expect("recovered leader info")
        .current_term;
    assert!(
        infos.iter().all(|info| info.current_term == recovered_term
            && info.commit_index == 2
            && info.last_applied == 2
            && info.log_len == 3),
        "all nodes must converge on the new leader's term and log: {infos:?}"
    );
    assert!(
        recovered_term >= new_term,
        "recovery must not return to the old term: {recovered_term} < {new_term}"
    );

    abort_all(&mut nodes);
}
