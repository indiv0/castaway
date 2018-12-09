extern crate castaway;
extern crate libc;

use libc::c_void;
use std::cell::RefCell;
use std::collections::HashSet;
use std::ptr;
use castaway::{
    Callbacks,
    Id,
    MessageAppendEntriesRaw,
    MessageRequestVote,
    RaftServer,
};

#[derive(Debug, PartialEq)]
enum Message {
    AppendEntries(MessageAppendEntriesRaw),
    RequestVote(MessageRequestVote),
}

thread_local!{
    static SENT_MESSAGES: RefCell<Vec<(usize, Message)>> = RefCell::new(Vec::new());
}

extern fn mock_send_append_entries(_raft: *mut RaftServer, _udata: *mut c_void, peer: usize, msg: *mut MessageAppendEntriesRaw) {
    let msg = unsafe {
        (*msg).clone()
    };
    SENT_MESSAGES.with(|prev| {
        (*prev.borrow_mut()).push((peer, Message::AppendEntries(msg)));
    });
}
extern fn mock_send_request_vote(_raft: *mut RaftServer, _udata: *mut c_void, peer: usize, msg: *mut MessageRequestVote) {
    let msg = unsafe {
        (*msg).clone()
    };
    SENT_MESSAGES.with(|prev| {
        (*prev.borrow_mut()).push((peer, Message::RequestVote(msg)));
    });
}

const MOCK_CALLBACKS: Callbacks = Callbacks {
    send_append_entries: mock_send_append_entries,
    send_request_vote: mock_send_request_vote,
};

#[test]
fn test_leader_election() {
    // FIXME
    let server_ids: HashSet<Id> = (0..5).collect();
    let mut servers: Vec<RaftServer> = {
        (0..5).clone()
            .map(|id| RaftServer::new(id))
            .map(|mut raft| {
                raft.register_callbacks(MOCK_CALLBACKS, ptr::null_mut());
                raft
            })
            .collect()
    };
    for server in servers.iter_mut() {
        for id in server_ids.clone() {
            if id != server.id() {
                server.add_peer(id, ptr::null_mut());
            }
        }
    }

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
    assert_eq!(servers[0].try_become_leader(), Ok(true));
    assert!(servers[0].is_leader());
}
