#![allow(dead_code, unused_variables)]

type Id = usize;
type Command = ();
type Term = usize;
/// An individual log entry.
///
/// Each entry contains a command for the state machine, and the term when the
/// entry was received by the leader (first index is 1).
type LogEntry = (Command, Term);
/// Log to replicate across servers.
type Log = Vec<LogEntry>;

/// Message invoked by leader to replicate log entries (§5.3); also used as
/// heartbeat (§5.2).
#[derive(Debug)]
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

    fn set_state(&mut self, state: RaftState) {
        self.state = state;
    }

    fn recv_append_entries(&mut self, peer: &Id, msg: MessageAppendEntries) {}

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
    fn test_raft_server_set_state() {
        let mut raft = RaftServer::new();
        assert_eq!(raft.state, RaftState::Follower);
        raft.set_state(RaftState::Leader);
        assert_eq!(raft.state, RaftState::Leader);
    }
}
