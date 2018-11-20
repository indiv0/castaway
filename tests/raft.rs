extern crate castaway;

use std::collections::HashSet;
use castaway::{Id, Message, RaftServer, ReceiveResult};

#[test]
fn test_leader_election() {
    let mut servers: Vec<RaftServer> = {
        let server_ids: HashSet<Id> = (0..5).collect();
        (0..5).clone()
            .map(|id| RaftServer::new(id, server_ids.clone()))
            .collect()
    };

    servers[0].timeout().unwrap();
    assert!(servers[0].is_candidate());
    for server in servers[1..5].iter() {
        assert!(server.is_follower());
    }

    // Vote for self
    let req = servers[0].request_vote(&0).unwrap();
    let res = match servers[0].receive(&0, &Message::RequestVote(req)) {
        ReceiveResult::Response(res) => res,
        _ => unreachable!(),
    };
    assert_eq!(servers[0].receive(&0, &res), ReceiveResult::ResponseProcessed);
    assert_eq!(servers[0].voted_for(), Some(0));
    assert!(servers[0].is_candidate());

    // Get one vote from another server.
    let req = servers[0].request_vote(&1).unwrap();
    match servers[1].receive(&0, &Message::RequestVote(req.clone())) {
        ReceiveResult::UpdatedTerm => {},
        _ => unreachable!(),
    };
    let res = match servers[1].receive(&0, &Message::RequestVote(req)) {
        ReceiveResult::Response(res) => res,
        _ => unreachable!(),
    };
    assert_eq!(servers[0].receive(&1, &res), ReceiveResult::ResponseProcessed);
    assert_eq!(servers[1].voted_for(), Some(0));
    assert!(servers[0].is_candidate());

    // Get third vote to reach majority.
    let req = servers[0].request_vote(&2).unwrap();
    match servers[2].receive(&0, &Message::RequestVote(req.clone())) {
        ReceiveResult::UpdatedTerm => {},
        _ => unreachable!(),
    };
    let res = match servers[2].receive(&0, &Message::RequestVote(req)) {
        ReceiveResult::Response(res) => res,
        _ => unreachable!(),
    };
    assert_eq!(servers[0].receive(&2, &res), ReceiveResult::ResponseProcessed);
    assert_eq!(servers[2].voted_for(), Some(0));
    assert!(servers[0].is_candidate());


    // TODO: enforce the invariant that we *must* call this eventually.
    servers[0].become_leader();
    assert!(servers[0].is_leader());
}
