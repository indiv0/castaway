#![allow(dead_code, unused_variables)]

use std::cmp;

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
#[derive(Debug)]
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
#[derive(Debug)]
struct MessageRequestVoteResponse {
    /// `current_term`, for candidate to update itself.
    term: Term,
    /// `true` means candidate received vote.
    vote_granted: bool,
}

/// Volatile state on leader nodes.
#[derive(Debug)]
struct Leader {
    /// For each server, index of next log entry to send to that server.
    ///
    /// Initialized to leader last log index + 1.
    next_index: Vec<usize>,
    /// For each server, index of highest log entry known to be replicated on server.
    ///
    /// Initialized to 0, increases monotonically.
    match_index: Vec<usize>,
}

#[derive(Debug, PartialEq)]
enum RaftState {
    _Candidate,
    Follower,
    Leader,
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

    fn set_commit_index(&mut self, index: usize) {
        self.commit_index = index;
    }

    fn set_state(&mut self, state: RaftState) {
        self.state = state;
    }

    fn recv_append_entries(&mut self, peer: &Id, mut msg: MessageAppendEntries) -> MessageAppendEntriesResponse {
        let mut resp = MessageAppendEntriesResponse {
            term: self.current_term,
            success: false,
        };

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

    fn recv_append_entries_response(&mut self, peer: &Id, msg: MessageAppendEntriesResponse) {}

    fn recv_request_vote(&mut self, peer: &Id, msg: MessageRequestVote) {}

    fn recv_request_vote_response(&mut self, peer: &Id, msg: MessageRequestVoteResponse) {}
}

/// Initializes a new, empty, replicated log.
fn log_new() -> Log {
    Vec::new()
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
        assert_eq!(raft.state, RaftState::Follower);
        raft.set_state(RaftState::Leader);
        assert_eq!(raft.state, RaftState::Leader);
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
        raft.set_current_term(7);
        let aer = raft.recv_append_entries(&1, ae);

        assert_eq!(aer.term, 7);
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

        println!("log: {:?}", raft.log);
        assert_eq!(raft.commit_index, 5);;
    }
}
