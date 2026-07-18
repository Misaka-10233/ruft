mod common;

use common::{
    PartitionController, abort_all, assert_no_apply, collect_infos, node_by_id, node_mut_by_id,
    recv_command, start_cluster, wait_for_commit_among, wait_for_leader_among,
};
use ruft::result::AppendResult;
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_leader_uncommitted_tail_is_replaced_by_majority_log() {
    let members = vec![31_001, 31_002, 31_003];
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

    controller.partition(&[old_leader_id], &majority);
    let rejected_command = b"old leader uncommitted tail".to_vec();
    assert!(matches!(
        node_by_id(&nodes, old_leader_id)
            .handle
            .append_log(rejected_command.clone())
            .await
            .expect("append to isolated leader"),
        AppendResult::Accepted { index: 1, .. }
    ));
    assert_no_apply(
        node_mut_by_id(&mut nodes, old_leader_id),
        Duration::from_millis(400),
    )
    .await;
    assert_eq!(
        node_by_id(&nodes, old_leader_id)
            .handle
            .get_info()
            .await
            .expect("isolated leader info")
            .commit_index,
        0,
        "a one-node partition must not commit its local entry"
    );

    let new_leader_id = wait_for_leader_among(&nodes, &majority).await;
    let replacement = b"majority replacement".to_vec();
    assert!(matches!(
        node_by_id(&nodes, new_leader_id)
            .handle
            .append_log(replacement.clone())
            .await
            .expect("append to majority leader"),
        AppendResult::Accepted { index: 1, term } if term > old_term
    ));
    wait_for_commit_among(&nodes, &majority, 1).await;
    for id in &majority {
        assert_eq!(
            recv_command(node_mut_by_id(&mut nodes, *id)).await,
            (1, replacement.clone()),
            "majority node {id} applied replacement"
        );
    }

    controller.heal_all();
    wait_for_commit_among(&nodes, &members, 1).await;
    assert_eq!(
        recv_command(node_mut_by_id(&mut nodes, old_leader_id)).await,
        (1, replacement.clone()),
        "the former leader must apply the majority entry, never its uncommitted tail"
    );

    let infos = collect_infos(&nodes).await;
    for info in &infos {
        assert_eq!(info.commit_index, 1, "node {} commit index", info.node_id);
        assert_eq!(info.last_applied, 1, "node {} applied index", info.node_id);
        assert_eq!(info.log_len, 2, "node {} log length", info.node_id);
    }
    assert!(
        infos.iter().all(|info| info.current_term > old_term),
        "the repaired cluster must retain the higher majority term: {infos:?}"
    );

    abort_all(&mut nodes);
}
