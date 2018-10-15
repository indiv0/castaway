#![allow(dead_code, unused_variables)]

use std::cmp::{self, Ordering};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

type Id = usize;
type Command = ();
type Term = usize;
/// An individual log entry.
///
/// Each entry contains a command for the state machine, and the term when the
/// entry was received by the leader (first index is 1).
#[derive(Clone, Debug, PartialEq)]
struct LogEntry(Command, Term);
/// Log to replicate across servers.
type Log = Vec<LogEntry>;

impl LogEntry {
    fn term(&self) -> Term {
        self.1
    }
}

/// Message invoked by leader to replicate log entries (§5.3); also used as
/// heartbeat (§5.2).
#[derive(Clone, Debug)]
struct MessageAppendEntries {
    /// Leader's term.
    term: Term,
    /// Used so follower nodes can redirect clients to the leader.
    leader_id: Id,
    /// Index of log entry immediately preceding new ones.
    prev_log_index: usize,
    /// Term of previous `prev_log_index` entry.
    prev_log_term: Term,
    /// Log entries to store.
    ///
    /// Empty for heartbeat messages; may send more than one for efficiency.
    entries: Vec<LogEntry>,
    /// Leader's `commit_index`.
    leader_commit: usize,
}

/// Message issued in response to an `AppendEntries` RPC.
#[derive(Debug)]
struct MessageAppendEntriesResponse {
    /// `current_term`, for leader to update itself.
    term: Term,
    /// `true` if follower contained entry matching `prev_log_index` and `prev_log_term`.
    success: bool,
}

/// Message invoked by candidates to gather votes (§5.2).
#[derive(Clone, Debug)]
struct MessageRequestVote {
    /// Candidate's term.
    term: Term,
    /// Candidate requesting vote.
    candidate_id: Id,
    /// Index of candidate's last log entry.
    last_log_index: usize,
    /// Term of candidate's last log entry.
    last_log_term: usize,
}

/// Message issued in response to a `RequestVote` RPC.
#[derive(Clone, Debug)]
struct MessageRequestVoteResponse {
    /// `current_term`, for candidate to update itself.
    term: Term,
    /// `true` means candidate received vote.
    vote_granted: bool,
}

/// Volatile state on candidate nodes.
///
/// This isn't formally specified in the Raft paper, but it is useful for
/// tracking election status on a per-candidate basis.
#[derive(Clone, Debug, PartialEq)]
struct CandidateState {
    /// Servers which have responded to a RequestVote RPC call.
    votes_responded: HashSet<Id>,
    /// Servers which have confirmed their vote for this candidate this term.
    votes_granted: HashSet<Id>,
}

/// Volatile state on leader nodes.
#[derive(Clone, Debug, PartialEq)]
struct LeaderState {
    /// For each server, index of next log entry to send to that server.
    ///
    /// Initialized to leader last log index + 1.
    next_index: HashMap<Id, usize>,
    /// For each server, index of highest log entry known to be replicated on server.
    ///
    /// Initialized to 0, increases monotonically.
    match_index: HashMap<Id, usize>,
}

#[derive(Debug, PartialEq)]
enum RaftState {
    Candidate(CandidateState),
    Follower,
    Leader(LeaderState),
}

#[derive(Debug)]
struct RaftServer {
    /// Latest term server has seen.
    ///
    /// Initialized to 0 on first boot, increases monotonically.
    current_term: Term,
    /// Candidate that the server voted for in the current term.
    ///
    /// `None` if server hasn't voted.
    voted_for: Option<Id>,
    /// Replicated log.
    log: Log,
    /// Index of highest log entry known to be committed.
    ///
    /// Initialized to 0, increases monotonically.
    commit_index: usize,
    /// Index of highest log entry applied to state machine.
    ///
    /// Initialized to 0, increases monotonically.
    last_applied: usize,

    /// Raft state of this server (e.g. candidate/follower/leader).
    state: RaftState,
}

impl RaftServer {
    /// Instantiate a new Raft server.
    fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: log_new(),
            commit_index: 0,
            last_applied: 0,
            state: RaftState::Follower,
        }
    }

    fn set_current_term(&mut self, term: usize) {
        self.current_term = term;
    }

    fn set_voted_for(&mut self, id: Option<Id>) {
        self.voted_for = id;
    }

    fn set_commit_index(&mut self, index: usize) {
        self.commit_index = index;
    }

    fn set_state(&mut self, state: RaftState) {
        self.state = state;
    }

    /// Candidate transitions to leader.
    ///
    /// # Panics
    ///
    /// Panics if the server is not currently in the candidate state.
    /// Panics if the server does not have enough votes to form a quorum.
    fn become_leader(&mut self, servers: &HashSet<Id>) {
        {
            let state = match self.state {
                RaftState::Candidate(ref state) => state,
                _ => panic!("become_leader called on non-candidate node"),
            };

            if !is_quorum(&state.votes_granted, servers) {
                panic!("insufficient votes for quorum");
            }
        }

        self.state = RaftState::Leader(LeaderState {
            next_index: servers.iter().cloned().map(|s| (s, self.log.len() + 1)).collect(),
            match_index: servers.iter().cloned().map(|s| (s, 0)).collect(),
        });
    }

    fn recv_append_entries(&mut self, peer: &Id, mut msg: MessageAppendEntries) -> MessageAppendEntriesResponse {
        let mut resp = MessageAppendEntriesResponse {
            term: self.current_term,
            success: false,
        };

        if msg.term < self.current_term {
            return resp;
        }

        {
            let prev_log_entry = match self.log.get(msg.prev_log_index - 1) {
                Some(entry) => entry,
                None => return resp,
            };

            if prev_log_entry.term() != msg.prev_log_term {
                return resp;
            }
        }

        let num_matching_new_entries = self.log.iter()
            .skip(msg.prev_log_index)
            .zip(&msg.entries)
            .take_while(|(entry, new_entry)| {
                entry.term() == new_entry.term()
            })
            .count();
        let last_matching_index = msg.prev_log_index + num_matching_new_entries;
        if last_matching_index < self.log.len() {
            self.log.split_off(last_matching_index);
        }

        let mut new_entries = msg.entries.split_off(num_matching_new_entries);
        self.log.append(&mut new_entries);

        if msg.leader_commit > self.commit_index {
            let index_of_last_new_entry = self.log.len();
            self.set_commit_index(cmp::min(msg.leader_commit, index_of_last_new_entry));
        }

        resp.success = true;
        resp
    }

    /// Server receives an AppendEntries response from `peer` with `msg.term ==
    /// self.current_term`.
    ///
    /// # Panics
    ///
    /// Panics if the server handling the response is not currently a leader.
    /// Panics if the server's state does not have a `next_index` entry for the
    /// specified peer.
    /// Panics if the server's state does not have a `match_index` entry for the
    /// specified peer.
    fn recv_append_entries_response(&mut self, peer: &Id, msg: MessageAppendEntriesResponse) {
        // `term` should be equal to `current_term` as the server issuing the
        // response should've increased its term to match that of the issued
        // request, and as the request came from this server, it should match
        // `current_term`.
        assert!(msg.term == self.current_term);

        match self.state {
            RaftState::Leader(ref mut state) => {
                match state.next_index.get_mut(peer) {
                    Some(next_index) => {
                        if msg.success {
                            *next_index = *next_index + 1;
                        } else {
                            *next_index = cmp::max(*next_index - 1, 1);
                            // `match_index` does not have to be modified if the
                            // RPC call was unsuccessful, so we can return
                            // early.
                            return;
                        }
                    },
                    None => panic!("Leader state missing next_index for peer {}", peer),
                };

                match state.match_index.get_mut(peer) {
                    Some(match_index) => {
                        assert!(msg.success);
                        *match_index = *match_index + 1;
                    },
                    None => panic!("Leader state missing match_index for peer {}", peer),
                };
            },
            _ => panic!("Non-leader node handling AppendEntries response"),
        }
    }

    /// Server receives a RequestVote request from server `peer` with `msg.term
    /// <= self.current_term`.
    fn recv_request_vote(&mut self, peer: &Id, msg: MessageRequestVote) -> MessageRequestVoteResponse {
        // `term` should never be greater than `current_term` as  `current_term`
        // should've been advanced after receiving the RPC but prior to calling
        // this handler.
        assert!(msg.term <= self.current_term);

        let mut resp = MessageRequestVoteResponse {
            term: self.current_term,
            vote_granted: false,
        };

        if msg.term < self.current_term {
            return resp;
        }

        match self.voted_for {
            Some(candidate_id) if candidate_id == msg.candidate_id => {},
            None => {},
            _ => return resp,
        }

        // Determine if the candidate's or the receiver's log is more up-to-date.
        // The more up-to-date of the two logs is the log which:
        // 1. has the later term, if the last entries in the logs have different
        //    terms; otherwise,
        // 2. whichever log is longer is more up-to-date.
        match msg.last_log_term.cmp(&last_term(&self.log)) {
            Ordering::Less => {
                return resp;
            },
            Ordering::Equal => if msg.last_log_index < self.log.len() {
                return resp;
            },
            _ => {},
        }

        // Update the candidate that was voted for.
        self.set_voted_for(Some(msg.candidate_id));

        resp.vote_granted = true;
        resp
    }

    /// Server receives a RequestVote response from server `peer` wtih
    /// `msg.term == self.current_term`.
    fn recv_request_vote_response(&mut self, peer: &Id, msg: MessageRequestVoteResponse) {
        // `term` should be equal to `current_term` as the server issuing the
        // response should've increased its term to match that of the issued
        // request, and as the request came from this server, it should match
        // `current_term`.
        assert!(msg.term == self.current_term);

        // Mark `peer` as having responded to our RequestVote RPC.
        match self.state {
            RaftState::Candidate(ref mut state) => {
                let not_present = state.votes_responded.insert(*peer);
                // The vote MUST not have already been present.
                assert!(not_present);

                // If `peer` granted the candidate their vote, add it to the
                // list.
                if msg.vote_granted {
                    let not_present = state.votes_granted.insert(*peer);
                    // The vote MUST not have already been present.
                    assert!(not_present);
                }
            },
            _ => {},
        }
    }
}

/// Initializes a new, empty, replicated log.
fn log_new() -> Log {
    Vec::new()
}

/// Returns true if a subset of a set constitutes a quorum (i.e., a majority).
///
/// # Assumptions
///
/// It is assumed that `subset` is a subset of `set`.
fn is_quorum<T>(subset: &HashSet<T>, set: &HashSet<T>) -> bool
    where T: Eq + Hash,
{
    assert!(subset.is_subset(set));
    subset.len() * 2 > set.len()
}

/// The term of the last entry in a log, or 0 if the log is empty.
fn last_term(log: &Log) -> usize {
    match log.len() {
        0 => 0,
        x => log[x - 1].term()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_server_new() {
        let raft = RaftServer::new();
        // Term and index values MUST be initialized to 0 on first boot.
        assert_eq!(raft.current_term, 0);
        assert_eq!(raft.commit_index, 0);
        assert_eq!(raft.last_applied, 0);

        assert_eq!(raft.state, RaftState::Follower);
    }

    #[test]
    fn test_raft_server_set_current_term() {
        let mut raft = RaftServer::new();
        assert_eq!(raft.current_term, 0);
        raft.set_current_term(5);
        assert_eq!(raft.current_term, 5);
    }

    #[test]
    fn test_raft_server_set_state() {
        let mut raft = RaftServer::new();

        let leader_state = LeaderState {
            match_index: HashMap::new(),
            next_index: HashMap::new(),
        };
        assert_eq!(raft.state, RaftState::Follower);
        raft.set_state(RaftState::Leader(leader_state.clone()));
        assert_eq!(raft.state, RaftState::Leader(leader_state));
    }

    /* `RaftServer::become_leader` tests */

    #[test]
    fn test_raft_server_become_leader() {
        let servers = [0, 1, 2, 3, 4].iter().cloned().collect();

        let mut raft = RaftServer::new();
        raft.state = RaftState::Candidate(CandidateState {
            votes_responded: [0, 1, 2, 3].iter().cloned().collect(),
            votes_granted: [0, 1, 2].iter().cloned().collect(),
        });
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
        ]);

        raft.become_leader(&servers);
        assert_eq!(raft.state, RaftState::Leader(LeaderState {
            next_index: [(0, 4), (1, 4), (2, 4), (3, 4), (4, 4)].iter().cloned().collect(),
            match_index: [(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)].iter().cloned().collect(),
        }));
    }

    #[test]
    #[should_panic(expected = "become_leader called on non-candidate node")]
    fn test_raft_server_become_leader_panic_already_leader() {
        let mut raft = RaftServer::new();

        // `state` is `Leader`
        raft.state = RaftState::Leader(LeaderState {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        });
        raft.become_leader(&HashSet::new());
    }

    #[test]
    #[should_panic(expected = "become_leader called on non-candidate node")]
    fn test_raft_server_become_leader_panic_state_is_follower() {
        let mut raft = RaftServer::new();

        // `state` is `Follower`
        raft.state = RaftState::Follower;
        raft.become_leader(&HashSet::new());
    }

    /* AppendEntries RPC tests */

    // Reply false if `term` < `current_term` (§5.1).
    #[test]
    fn test_raft_server_recv_append_entries_reply_false_when_term_less_than_current_term() {
        let ae = MessageAppendEntries {
            term: 1,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        let mut raft = RaftServer::new();

        // `current_term` is higher than `term`
        raft.set_current_term(5);
        let aer = raft.recv_append_entries(&1, ae);
        assert_eq!(aer.term, 5);
        assert_eq!(aer.success, false);
    }

    // Reply false if log doesn't contain an entry at `prev_log_index` whose
    // term matches `prev_log_term` (§5.3).
    #[test]
    fn test_raft_server_recv_append_entries_reply_false_when_log_doesnt_contain_term_matching_prev_log_term() {
        let ae = MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 4,
            entries: Vec::new(),
            leader_commit: 0,
        };

        let mut raft = RaftServer::new();

        // `log` does not contain an entry at `prev_log_index`
        let aer = raft.recv_append_entries(&1, ae.clone());

        assert_eq!(aer.success, false);

        // `log` does not contain an entry at `prev_log_index` whose term
        // matches `prev_log_term`
        // TODO: add these entries via an RPC call instead.
        raft.log.push(LogEntry((), 3));
        let aer = raft.recv_append_entries(&1, ae);

        assert_eq!(aer.success, false);
    }

    // If an existing entry conflicts with a new one (same index but different
    // terms), delete the existing entry and all that follow it (§5.3).
    #[test]
    fn test_raft_server_recv_append_entries_existing_entry_conflicts_with_new_one() {
        let ae = MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry((), 2), LogEntry((), 4)],
            leader_commit: 0,
        };

        let mut raft = RaftServer::new();

        // Existing entry conflicts with a new one.
        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 3),
        ]);
        let aer = raft.recv_append_entries(&1, ae);

        assert_eq!(aer.success, true);
        assert_eq!(raft.log, vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 4),
        ]);
    }

    // Append any new entries not already in the log.
    #[test]
    fn test_raft_server_recv_append_entries_append_new_entries() {
        let ae = MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry((), 2), LogEntry((), 3), LogEntry((), 4)],
            leader_commit: 0,
        };

        let mut raft = RaftServer::new();

        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
        ]);
        raft.set_current_term(4);
        let aer = raft.recv_append_entries(&1, ae);

        assert_eq!(aer.term, 4);
        assert_eq!(aer.success, true);
        assert_eq!(raft.log, vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 4),
        ]);
    }

    // If `leader_commit` > `commit_index`, set `commit_index =
    // min(leader_commit, index of last new entry)`.
    #[test]
    fn test_raft_server_recv_append_entries_leader_commit_greater_than_commit_index() {
        let mut ae = MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![
                LogEntry((), 2),
                LogEntry((), 4),
                LogEntry((), 4),
                LogEntry((), 4),
            ],
            leader_commit: 3,
        };

        let mut raft = RaftServer::new();

        // `leader_commit` is greater than `commit_index`, and less than index
        // of last new entry
        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 3),
        ]);
        raft.set_commit_index(2);
        raft.recv_append_entries(&1, ae.clone());

        assert_eq!(raft.commit_index, 3);

        // `leader_commit` is greater than `commit_index`, and greater than
        // index of last new entry
        raft.set_commit_index(2);
        ae.leader_commit = 7;
        raft.recv_append_entries(&1, ae);

        assert_eq!(raft.commit_index, 5);;
    }

    /* AppendEntries RPC response tests */

    #[test]
    #[should_panic]
    fn test_raft_server_recv_append_entries_response_invalid_term() {
        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: true,
        };

        let mut raft = RaftServer::new();

        // `current_term` is not equal to `term`
        raft.set_current_term(0);
        raft.recv_append_entries_response(&1, aer);
    }

    #[test]
    fn test_raft_server_recv_append_entries_response_success() {
        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: true,
        };

        let mut raft = RaftServer::new();
        raft.set_current_term(1);
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 3),
        ]);
        // NOTE: we're assuming here that there are 3 Raft peers: 0
        // (this server), 1 (the one sending the response), and 2.
        raft.state = RaftState::Leader(LeaderState {
            next_index: [(1, 7), (2, 7)].iter().cloned().collect(),
            match_index: [(1, 0), (2, 0)].iter().cloned().collect(),
        });

        raft.recv_append_entries_response(&1, aer);
        assert_eq!(raft.state, RaftState::Leader(LeaderState {
            next_index: [(1, 8), (2, 7)].iter().cloned().collect(),
            match_index: [(1, 1), (2, 0)].iter().cloned().collect(),
        }));
    }

    #[test]
    fn test_raft_server_recv_append_entries_response_not_success() {
        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: false,
        };

        let mut raft = RaftServer::new();
        raft.set_current_term(1);
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 3),
        ]);
        // NOTE: we're assuming here that there are 3 Raft peers: 0
        // (this server), 1 (the one sending the response), and 2.
        raft.state = RaftState::Leader(LeaderState {
            next_index: [(1, 7), (2, 7)].iter().cloned().collect(),
            match_index: [(1, 0), (2, 0)].iter().cloned().collect(),
        });

        raft.recv_append_entries_response(&1, aer);
        assert_eq!(raft.state, RaftState::Leader(LeaderState {
            next_index: [(1, 6), (2, 7)].iter().cloned().collect(),
            match_index: [(1, 0), (2, 0)].iter().cloned().collect(),
        }));
    }

    /* RequestVote RPC tests */

    // Reply false if `term` < `current_term` (§5.1).
    #[test]
    fn test_raft_server_recv_request_vote_reply_false_when_term_less_than_current_term() {
        let rv = MessageRequestVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 5,
            last_log_term: 5,
        };

        let mut raft = RaftServer::new();

        // `current_term` is higher than `term`
        raft.set_current_term(5);
        let rvr = raft.recv_request_vote(&1, rv);
        assert_eq!(rvr.term, 5);
        assert_eq!(rvr.vote_granted, false);
        assert_eq!(raft.voted_for, None);
    }

    /*
     * If `voted_for` is `None` or `candidate_id`, and candidate's log is at
     * least as up-to-date as receiver's log, grant vote (§5.2, §5.4).
     */

    #[test]
    fn test_raft_server_recv_request_vote_reply_false_when_voted_for_is_not_candidate_id() {
        let rv = MessageRequestVote {
            term: 5,
            candidate_id: 1,
            last_log_index: 5,
            last_log_term: 5,
        };

        let mut raft = RaftServer::new();

        // `voted_for` does not match `candidate_id`
        raft.voted_for = Some(3);
        raft.current_term = 5;
        let rvr = raft.recv_request_vote(&1, rv);
        assert_eq!(rvr.term, 5);
        assert_eq!(rvr.vote_granted, false);
        assert_eq!(raft.voted_for, Some(3));
    }

    #[test]
    fn test_raft_server_recv_request_vote_reply_false_if_candidates_log_is_not_up_to_date() {
        let rv = MessageRequestVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 3,
            last_log_term: 3,
        };

        let mut raft = RaftServer::new();

        // TODO: replace this with a call to `update_term`.
        raft.current_term = 1;
        // candidate's log is not as up-to-date as the receiver's log as the
        // last entry in the receiver's log has a later term.
        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 4),
        ]);
        let rvr = raft.recv_request_vote(&1, rv.clone());
        assert_eq!(rvr.term, 1);
        assert_eq!(rvr.vote_granted, false);
        assert_eq!(raft.voted_for, None);

        // candidate's log is not as up-to-date as the receiver's log as the
        // receiver's log is longer.
        // TODO: add these entries via an RPC call instead.
        raft.log = vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 4),
        ];
        let rvr = raft.recv_request_vote(&1, rv);
        assert_eq!(rvr.term, 1);
        assert_eq!(rvr.vote_granted, false);
        assert_eq!(raft.voted_for, None);
    }

    #[test]
    fn test_raft_server_recv_request_vote_grant_vote() {
        let rv = MessageRequestVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 3,
            last_log_term: 3,
        };

        let mut raft = RaftServer::new();

        // TODO: replace this with a call to `update_term`.
        raft.current_term = 1;
        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
        ]);
        let rvr = raft.recv_request_vote(&1, rv);
        assert_eq!(rvr.term, 1);
        assert_eq!(rvr.vote_granted, true);
        assert_eq!(raft.voted_for, Some(1));
    }

    /* RequestVote RPC response tests */

    #[test]
    #[should_panic]
    fn test_raft_server_recv_request_vote_response_invalid_term() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: true,
        };

        let mut raft = RaftServer::new();

        // `current_term` is not equal to `term`
        raft.set_current_term(0);
        raft.recv_request_vote_response(&1, rvr);
    }

    #[test]
    fn test_raft_server_recv_request_vote_response_vote_granted() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: true,
        };

        let mut raft = RaftServer::new();
        // TODO: replace this with a call to `update_term`.
        raft.current_term = 1;
        raft.state = RaftState::Candidate(CandidateState {
            votes_responded: HashSet::new(),
            votes_granted: HashSet::new(),
        });

        assert_eq!(rvr.term, raft.current_term);
        raft.recv_request_vote_response(&1, rvr.clone());
        assert_eq!(raft.state,
            RaftState::Candidate(CandidateState {
                votes_responded: [1].iter().cloned().collect(),
                votes_granted: [1].iter().cloned().collect(),
            })
        );
    }

    #[test]
    fn test_raft_server_recv_request_vote_response_vote_not_granted() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: false,
        };

        let mut raft = RaftServer::new();
        // TODO: replace this with a call to `update_term`.
        raft.current_term = 1;
        raft.state = RaftState::Candidate(CandidateState {
            votes_responded: HashSet::new(),
            votes_granted: HashSet::new(),
        });

        assert_eq!(rvr.term, raft.current_term);
        raft.recv_request_vote_response(&1, rvr.clone());
        assert_eq!(raft.state,
            RaftState::Candidate(CandidateState {
                votes_responded: [1].iter().cloned().collect(),
                votes_granted: HashSet::new(),
            })
        );
    }

    /* Helper tests */

    #[test]
    fn test_last_term() {
        assert_eq!(last_term(&Vec::new()), 0);
        assert_eq!(last_term(&vec![LogEntry((), 0), LogEntry((), 1)]), 1);
    }

    #[test]
    fn test_is_quorum() {
        let servers = [0, 1, 2, 3, 4].iter().cloned().collect();

        assert!(!is_quorum(&[0, 1].iter().cloned().collect(), &servers));
        assert!(is_quorum(&[0, 1, 2].iter().cloned().collect(), &servers));
    }

    #[test]
    #[should_panic(expected = "assertion failed: subset.is_subset(set)")]
    fn test_is_quorum_panic_not_subset() {
        is_quorum(
            &[5].iter().cloned().collect(),
            &[0, 1, 2, 3, 4].iter().cloned().collect(),
        );
    }
}
