#[derive(PartialEq, Debug)]
pub(crate) enum Role {
    Leader,
    Follower,
    Candidate,
}
