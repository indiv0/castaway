extern crate castaway;

use std::collections::HashSet;
use castaway::{Id, RaftServer};

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
    let res = servers[0].handle_request_vote_request(&0, &req);
    servers[0].handle_request_vote_response(&0, &res);
    assert_eq!(servers[0].voted_for(), Some(0));
    assert!(servers[0].is_candidate());

    // Get one vote from another server.
    let req = servers[0].request_vote(&1).unwrap();
    servers[1].handle_request_vote_request(&0, &req.clone());
    let res = servers[1].handle_request_vote_request(&0, &req);
    servers[0].handle_request_vote_response(&1, &res);
    assert_eq!(servers[1].voted_for(), Some(0));
    assert!(servers[0].is_candidate());

    // Get third vote to reach majority.
    let req = servers[0].request_vote(&2).unwrap();
    servers[2].handle_request_vote_request(&0, &req.clone());
    let res = servers[2].handle_request_vote_request(&0, &req);
    servers[0].handle_request_vote_response(&2, &res);
    assert_eq!(servers[2].voted_for(), Some(0));
    assert!(servers[0].is_candidate());


    // TODO: enforce the invariant that we *must* call this eventually.
    servers[0].become_leader();
    assert!(servers[0].is_leader());
}
