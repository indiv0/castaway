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

/// Possible results once a message has been received.
#[derive(Debug, PartialEq)]
pub enum ReceiveResult {
    /// A response with a stale term was received, and so must be dropped.
    DropStaleResponse,
    /// An RPC request was processed, a response was generated and must be sent.
    // TODO: enforce the invariant that ONLY response-type messages should be
    // returned here.
    Response(Message),
    /// An RPC response was successfully processed.
    ResponseProcessed,
    /// An RPC with a newer term caused the recipient to advance its term first.
    UpdatedTerm,
}

/// RPC request or response message.
#[derive(Debug, PartialEq)]
pub enum Message {
    AppendEntries(MessageAppendEntries),
    AppendEntriesResponse(MessageAppendEntriesResponse),
    RequestVote(MessageRequestVote),
    RequestVoteResponse(MessageRequestVoteResponse),
}

impl Message {
    fn term(&self) -> usize {
        use self::Message::*;
        match self {
            AppendEntries(MessageAppendEntries { term, .. }) => *term,
            AppendEntriesResponse(MessageAppendEntriesResponse { term, .. }) => *term,
            RequestVote(MessageRequestVote { term, .. }) => *term,
            RequestVoteResponse(MessageRequestVoteResponse { term, .. }) => *term,
        }
    }
}

/// Message invoked by leader to replicate log entries (§5.3); also used as
/// heartbeat (§5.2).
#[derive(Clone, Debug, PartialEq)]
pub struct MessageAppendEntries {
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
#[derive(Debug, PartialEq)]
pub struct MessageAppendEntriesResponse {
    /// `current_term`, for leader to update itself.
    term: Term,
    /// `true` if follower contained entry matching `prev_log_index` and `prev_log_term`.
    success: bool,
}

/// Message invoked by candidates to gather votes (§5.2).
#[derive(Clone, Debug, PartialEq)]
pub struct MessageRequestVote {
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
#[derive(Clone, Debug, PartialEq)]
pub struct MessageRequestVoteResponse {
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

impl CandidateState {
    /// Initialize a new `CandidateState`.
    fn new(votes_responded: HashSet<Id>, votes_granted: HashSet<Id>) -> Self {
        Self {
            votes_responded,
            votes_granted,
        }
    }
}

impl Default for CandidateState {
    fn default() -> Self {
        Self::new(HashSet::new(), HashSet::new())
    }
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

/// Errors which may occur when attempting to send a RequestVote request.
#[derive(Debug, PartialEq)]
pub enum RequestVoteError {
    /// A RequestVote request may not be issued to a server which has already
    /// responded to a RequestVote request.
    AlreadyResponded,
    /// A non-candidate node cannot issue a RequestVote request.
    NotCandidate,
}

/// Errors which may occur when attempting to send an AppendEntries request.
#[derive(Debug, PartialEq)]
enum AppendEntriesError {
    /// Attempted to send an AppendEntries request to oneself or an unknown
    /// peer.
    InvalidPeer,
    /// A server must be a leader to send an AppendEntries request.
    NotLeader,
}

/// Errors which may occur when receiving a client request to add a value to the
/// log.
#[derive(Debug, PartialEq)]
enum ClientRequestError {
    /// A non-leader node received a client request.
    NotLeader,
}

/// Errors which may occur when issuing an election timeout for the server.
#[derive(Debug, PartialEq)]
pub enum TimeoutError {
    /// A leader may not experience an election timeout.
    IsLeader,
}

/// Errors which may occur when advancing the commit index for a leader node.
#[derive(Debug, PartialEq)]
enum AdvanceCommitIndexError {
    /// A non-leader node may not advance its commit index.
    NotLeader,
}

#[derive(Debug, PartialEq)]
enum RaftState {
    Candidate(CandidateState),
    Follower,
    Leader(LeaderState),
}

#[derive(Debug)]
pub struct RaftServer {
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

    /// ID of this server.
    id: Id,
    /// Raft state of this server (e.g. candidate/follower/leader).
    state: RaftState,
}

impl RaftServer {
    /// Instantiate a new Raft server.
    pub fn new(id: Id) -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: log_new(),
            commit_index: 0,
            last_applied: 0,
            id,
            state: RaftState::Follower,
        }
    }

    pub fn voted_for(&self) -> Option<Id> {
        self.voted_for
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

    /// Server restarts from stable storage.
    /// It loses everything but its `current_term`, `voted_for`, and `log`.
    fn restart(&mut self) {
        self.state = RaftState::Follower;
        self.commit_index = 0;
    }

    /// Server times out and starts a new election.
    pub fn timeout(&mut self) -> Result<(), TimeoutError> {
        if self.is_leader() {
            return Err(TimeoutError::IsLeader);
        }

        self.state = RaftState::Candidate(CandidateState::default());
        self.current_term += 1;
        // TODO: consider messaging localhost for setting the local vote, as
        // opposed to doing it atomically.
        self.voted_for = None;

        Ok(())
    }

    /// Candidate sends `peer` a RequestVote request.
    pub fn request_vote(&mut self, peer: &Id) -> Result<MessageRequestVote, RequestVoteError> {
        if !self.is_candidate() {
            return Err(RequestVoteError::NotCandidate);
        }

        match self.state {
            RaftState::Candidate(ref state) => {
                if state.votes_responded.contains(peer) {
                    return Err(RequestVoteError::AlreadyResponded);
                }
            },
            _ => unreachable!(),
        }

        Ok(MessageRequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_term: last_term(&self.log),
            last_log_index: self.log.len(),
        })
    }

    /// Leader sends `peer` an AppendEntries request containing up to 1 entry.
    ///
    /// # Notes
    ///
    /// While the Raft specification allows implementations to send more than 1
    /// at a time, this implementation follows the formal specification of just
    /// 1 because "it minimizes atomic regions without loss of generality".
    ///
    /// # Panics
    ///
    /// Panics if the `next_index` value for `peer` is not present.
    /// Panics if `next_index[peer] - 1` does not point to a valid log entry.
    fn append_entries(&self, peer: &Id) -> Result<MessageAppendEntries, AppendEntriesError> {
        if &self.id == peer {
            return Err(AppendEntriesError::InvalidPeer);
        }

        if !self.is_leader() {
            return Err(AppendEntriesError::NotLeader);
        }

        match self.state {
            RaftState::Leader(ref state) => {
                let next_index = match state.next_index.get(peer) {
                    Some(next_index) => *next_index,
                    None => panic!("Leader state missing next_index for peer {}", peer),
                };
                let prev_log_index = next_index - 1;
                let prev_log_term = if prev_log_index > 0 {
                    match self.log.get(prev_log_index - 1) {
                        Some(entry) => entry.term(),
                        None => panic!("Missing log entry"),
                    }
                } else {
                    0
                };

                // Send up to 1 entry, constrained by the end of the log.
                let last_entry = cmp::min(self.log.len(), next_index);
                // TODO: determine if `sub_seq` is really necessary here,
                // assuming that `next_index` and `last_entry` have sane values.
                // It should be possible to do this with simple slicing.
                let entries = sub_seq(&self.log, next_index, last_entry).to_vec();

                return Ok(MessageAppendEntries {
                    term: self.current_term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: cmp::min(self.commit_index, last_entry),
                });
            },
            _ => unreachable!(),
        }
    }

    /// Candidate transitions to leader.
    ///
    /// # Panics
    ///
    /// Panics if the server is not currently in the candidate state.
    /// Panics if the server does not have enough votes to form a quorum.
    pub fn become_leader(&mut self, servers: &HashSet<Id>) {
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

    /// Leader receives a client request to add `v` to the log.
    fn client_request(&mut self, v: Command) -> Result<(), ClientRequestError> {
        if !self.is_leader() {
            return Err(ClientRequestError::NotLeader);
        }

        let entry = LogEntry(v, self.current_term);
        self.log.push(entry);

        Ok(())
    }

    /// Leader advances its `commit_index`.
    ///
    /// # Note
    ///
    /// This is done as a separate step from handling `AppendEntries` responses,
    /// in part to minimize atomic regions, and in part so that leaders of
    /// single-server clusters are able to mark entries committed.
    ///
    /// # Panics
    ///
    /// Panics if the log entry at the index agreed upon by the servers cannot
    /// be found.
    // TODO: perhaps iterate over the keys of `self.state.match_index` instead
    // of passing in a server list?
    fn advance_commit_index(&mut self, servers: &HashSet<Id>) -> Result<(), AdvanceCommitIndexError> {
        if !self.is_leader() {
            return Err(AdvanceCommitIndexError::NotLeader);
        }

        let mut new_commit_index = self.commit_index;

        {
            let match_index_iter = match self.state {
                RaftState::Leader(ref state) => state.match_index.iter(),
                _ => unreachable!(),
            };

            let agree_indices = (1..self.log.len() + 1)
                .filter(|&idx| {
                    let agree_servers = match_index_iter
                        .clone()
                        .filter(|(&id, &m)| id == self.id || m >= idx)
                        .map(|(&id, _)| id)
                        .collect();
                    is_quorum(&agree_servers, servers)
                });
            if let Some(max_agree_index) = agree_indices.max() {
                let term = match self.log.get(max_agree_index - 1) {
                    Some(entry) => entry.term(),
                    None => panic!("Missing log entry"),
                };

                if term == self.current_term {
                    new_commit_index = max_agree_index;
                }
            }
        }

        self.commit_index = new_commit_index;

        Ok(())
    }

    /* Message handlers */

    /// Server receives an AppendEntries request from `peer` with `msg.term <=
    /// self.current_term`.
    ///
    /// Returns a response intended for the server which sent the request, if a
    /// response must be sent.
    ///
    /// # Note
    ///
    /// This just handles `msg.entries` of length 0 or 1, but the Raft formal
    /// specification states that implementations could safely accept more by
    /// treating them the same as independent requests of 1 entry.
    ///
    /// # Invariants
    ///
    /// 1. `msg.term <= self.current_term` must always be true when this
    /// function is called.
    /// 2. This function must never be called for a leader node.
    ///
    /// # Panics
    ///
    /// Panics if any invariants are violated.
    // TODO: ensure that only `msg.entries` of length `0` or `1` are handled.
    fn handle_append_entries_request(&mut self, peer: &Id, msg: &MessageAppendEntries) -> MessageAppendEntriesResponse {
        // Assert the invariants.
        assert!(msg.term <= self.current_term);
        // Non-leaders should not be issuing AppendEntries RPC calls.
        assert!(!self.is_leader());

        // If `msg.term < self.current_term` we reject the request.
        if msg.term < self.current_term {
            return MessageAppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        assert_eq!(msg.term, self.current_term);

        // If we're a candidate, we should return to the follower state.
        if self.is_candidate() {
            self.state = RaftState::Follower;
            return MessageAppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        // If we're not a candidate, we must be a follower as leaders ignore
        // incoming AppendEntries RPCs.
        assert!(self.is_follower());

        // Verify that our log history matches the history of the server which
        // issued the request.
        let log_ok =
        {
            if msg.prev_log_index == 0 {
                true
            } else if msg.prev_log_index > 0 {
                if msg.prev_log_index <= self.log.len() {
                    let prev_log_entry = match self.log.get(msg.prev_log_index - 1) {
                        Some(entry) => entry,
                        // TODO: this should be unreachable?
                        None => {
                            return MessageAppendEntriesResponse {
                                term: self.current_term,
                                success: false,
                            };
                        },
                    };

                    if msg.prev_log_term == prev_log_entry.term() {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        };

        // If the previous log records (as outlined in the incoming message) do
        // not match our existing log history, we reject the request.
        if !log_ok {
            return MessageAppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        assert!(log_ok);

        // At this point, the request has been accepted, but has not been
        // processed.

        // The index of the new entry to be inserted.
        let index = msg.prev_log_index + 1;

        // If the request contains no entries, it is a heartbeat, and is
        // accepted.
        if msg.entries.is_empty() {
            // This could make our `commit_index` decrease (e.g. if we
            // process an old, duplicated request), but that doesn't affect
            // anything.
            self.commit_index = cmp::min(msg.leader_commit, self.log.len());
            return MessageAppendEntriesResponse {
                term: self.current_term,
                success: true,
            };
        }

        assert!(!msg.entries.is_empty());

        // If our log does not already contain an entry at the new index,
        // then there can be no conflict and the entry may simply be
        // appended.
        if self.log.len() == msg.prev_log_index {
            // TODO: find a way to take ownership of `msg` and remove this
            // `clone`.
            self.log.push(msg.entries[0].clone());
            return MessageAppendEntriesResponse {
                term: self.current_term,
                success: true,
            };
        }

        // If our log already contains an entry at the new index, it's
        // either because: we've already appended it; or there is a
        // conflict.
        assert!(self.log.len() >= index);

        {
            // If the term of new entry and the one already in the log match,
            // that means the entry has already been appended. Otherwise,
            // there's a conflict.
            let entry = &self.log[index - 1];
            if entry.term() == msg.entries[0].term() {
                // This could make our `commit_index` decrease (e.g. if we
                // process an old, duplicated request), but that doesn't affect
                // anything.
                self.commit_index = cmp::min(msg.leader_commit, self.log.len());
                return MessageAppendEntriesResponse {
                    term: self.current_term,
                    success: true,
                };
            }

            assert!(entry.term() != msg.entries[0].term());
        }

        // When there's a conflict, we simply remove the last item in the log.
        let conflict_index = self.log.len();
        self.log.remove(conflict_index - 1);

        MessageAppendEntriesResponse {
            term: self.current_term,
            success: true,
        }
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
    fn handle_append_entries_response(&mut self, peer: &Id, msg: &MessageAppendEntriesResponse) {
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
    fn handle_request_vote_request(&mut self, peer: &Id, msg: &MessageRequestVote) -> MessageRequestVoteResponse {
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
    fn handle_request_vote_response(&mut self, peer: &Id, msg: &MessageRequestVoteResponse) {
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

    /// Any RPC with a newer `term` causes the recipient to advance to its term
    /// first.
    fn update_term(&mut self, peer: &Id, msg: &Message) {
        // The term only needs to be updated if the RPC's `term` is greater than
        // the server's `current_term`.
        assert!(msg.term() > self.current_term);

        self.current_term = msg.term();
        // Updating to a newer term puts the node into the follower state.
        self.state = RaftState::Follower;
        // Reset `voted_for` as we have not voted for anyone in the new term.
        self.voted_for = None;
    }

    /// Receive a message.
    ///
    /// # Returns
    ///
    /// Any RPC with a newer term causes the recipient to advance its term
    /// first, and will return `ReceiveResult::UpdatedTerm`.
    /// Responses with stale terms will return
    /// `ReceiveResult::DropStaleResponse` and must be ignored.
    /// Successfully processed RPC requests will return a
    /// `ReceiveResult::Response`, which must be sent to the requesting server.
    /// Successfully processed RPC responses will return a
    /// `ReceiveResult::ResponseProcessed`.
    pub fn receive(&mut self, peer: &Id, msg: &Message) -> ReceiveResult {
        if msg.term() > self.current_term {
            self.update_term(peer, msg);
            return ReceiveResult::UpdatedTerm;
        }

        use self::Message::*;
        match msg {
            AppendEntries(msg) => {
                let res = self.handle_append_entries_request(peer, msg);
                ReceiveResult::Response(Message::AppendEntriesResponse(res))
            },
            AppendEntriesResponse(msg) => {
                // Responses with stale items are ignored.
                if msg.term < self.current_term {
                    return ReceiveResult::DropStaleResponse;
                }

                self.handle_append_entries_response(peer, msg);
                ReceiveResult::ResponseProcessed
            },
            RequestVote(msg) => ReceiveResult::Response(Message::RequestVoteResponse(
                self.handle_request_vote_request(peer, msg)
            )),
            RequestVoteResponse(msg) => {
                // Responses with stale items are ignored.
                if msg.term < self.current_term {
                    return ReceiveResult::DropStaleResponse;
                }

                self.handle_request_vote_response(peer, msg);
                ReceiveResult::ResponseProcessed
            },
        }
    }

    /* Helpers */

    /// Returns `true` if the server is in the candidate state.
    pub fn is_candidate(&self) -> bool {
        match self.state {
            RaftState::Candidate(_) => true,
            _ => false,
        }
    }

    /// Returns `true` if the server is in the leader state.
    pub fn is_leader(&self) -> bool {
        match self.state {
            RaftState::Leader(_) => true,
            _ => false,
        }
    }

    /// Returns `true` if the server is in the follower state.
    pub fn is_follower(&self) -> bool {
        match self.state {
            RaftState::Follower => true,
            _ => false,
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

/// Returns the sequence <<s[m], s[m+1], ..., s[n]>>.
///
/// Out of bounds indices will be automatically corrected to be in bounds.
/// If `m > n`, an empty subsequence will be returned.
fn sub_seq<T>(s: &[T], m: usize, n: usize) -> &[T] {
    if m > n {
        &[]
    } else {
        let m = cmp::min(m, s.len() - 1);
        let n = cmp::min(n, s.len() - 1);
        &s[m..n+1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candidate_state_new() {
        assert_eq!(CandidateState::default(), CandidateState {
            votes_responded: HashSet::new(),
            votes_granted: HashSet::new(),
        });
    }

    #[test]
    fn test_raft_server_new() {
        let raft = RaftServer::new(0);
        // Term and index values MUST be initialized to 0 on first boot.
        assert_eq!(raft.current_term, 0);
        assert_eq!(raft.commit_index, 0);
        assert_eq!(raft.last_applied, 0);

        assert_eq!(raft.state, RaftState::Follower);
    }

    #[test]
    fn test_raft_server_set_current_term() {
        let mut raft = RaftServer::new(0);
        assert_eq!(raft.current_term, 0);
        raft.set_current_term(5);
        assert_eq!(raft.current_term, 5);
    }

    #[test]
    fn test_raft_server_set_state() {
        let mut raft = RaftServer::new(0);

        let leader_state = LeaderState {
            match_index: HashMap::new(),
            next_index: HashMap::new(),
        };
        assert_eq!(raft.state, RaftState::Follower);
        raft.set_state(RaftState::Leader(leader_state.clone()));
        assert_eq!(raft.state, RaftState::Leader(leader_state));
    }

    /* `RaftServer::restart` tests */

    #[test]
    fn test_raft_server_restart() {
        let mut raft = RaftServer::new(1);
        raft.current_term = 1;
        raft.voted_for = Some(3);
        raft.log.append(&mut vec![LogEntry((), 1)]);
        raft.state = RaftState::Candidate(CandidateState::default());
        raft.commit_index = 2;

        raft.restart();
        assert_eq!(raft.id, 1);
        assert_eq!(raft.current_term, 1);
        assert_eq!(raft.voted_for, Some(3));
        assert_eq!(raft.log, vec![LogEntry((), 1)]);
        assert_eq!(raft.state, RaftState::Follower);
        assert_eq!(raft.commit_index, 0);
    }

    /* `RaftServer::timeout` tests */

    #[test]
    fn test_raft_server_timeout() {
        let mut raft = RaftServer::new(0);
        raft.state = RaftState::Follower;
        raft.current_term = 0;
        raft.voted_for = Some(1);

        assert_eq!(raft.timeout(), Ok(()));
        assert_eq!(raft.state, RaftState::Candidate(CandidateState::default()));
        assert_eq!(raft.current_term, 1);
        assert_eq!(raft.voted_for, None);
    }

    #[test]
    fn test_raft_server_timeout_leader() {
        let mut raft = RaftServer::new(0);
        raft.state = RaftState::Leader(LeaderState {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        });

        assert_eq!(raft.timeout(), Err(TimeoutError::IsLeader));
    }

    /* `RaftServer::request_vote` tests */

    #[test]
    fn test_raft_server_request_vote() {
        let mut raft = RaftServer::new(0);
        raft.current_term = 5;
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 1),
            LogEntry((), 4),
        ]);
        raft.state = RaftState::Candidate(CandidateState::default());

        assert_eq!(raft.request_vote(&1), Ok(MessageRequestVote {
            term: 5,
            candidate_id: 0,
            last_log_term: 4,
            last_log_index: 3,
        }));
    }

    #[test]
    fn test_raft_server_request_vote_not_candidate() {
        let mut raft = RaftServer::new(0);

        assert_eq!(raft.request_vote(&1), Err(RequestVoteError::NotCandidate));
    }

    #[test]
    fn test_raft_server_request_vote_already_voted() {
        let mut raft = RaftServer::new(0);
        raft.state = RaftState::Candidate(CandidateState {
            votes_responded: [1].iter().cloned().collect(),
            ..Default::default()
        });

        assert_eq!(raft.request_vote(&1), Err(RequestVoteError::AlreadyResponded));
    }

    /* `RaftServer::append_entries` tests */

    #[test]
    fn test_raft_server_append_entries_two_missing() {
        let mut raft = RaftServer::new(0);
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
        ]);
        raft.current_term = 5;
        raft.state = RaftState::Leader(LeaderState {
            next_index: [(1, 3)].iter().cloned().collect(),
            match_index: [(1, 0)].iter().cloned().collect(),
        });

        let mut raft1 = RaftServer::new(1);
        raft1.current_term = 5;

        let req = raft.append_entries(&1);
        assert_eq!(req, Ok(MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 2,
            prev_log_term: 2,
            entries: Vec::new(),
            leader_commit: 0,
        }));
        let res = raft1.handle_append_entries_request(&0, &req.unwrap());
        raft.handle_append_entries_response(&1, &res);
        assert_eq!(raft.state, RaftState::Leader(LeaderState {
            next_index: [(1, 2)].iter().cloned().collect(),
            match_index: [(1, 0)].iter().cloned().collect(),
        }));
    }

    #[test]
    fn test_raft_server_append_entries_one_missing() {
        let mut raft = RaftServer::new(0);
        raft.log.append(&mut vec![
            LogEntry((), 1),
        ]);
        raft.current_term = 5;
        raft.state = RaftState::Leader(LeaderState {
            next_index: [(1, 2)].iter().cloned().collect(),
            match_index: [(1, 0)].iter().cloned().collect(),
        });

        let mut raft1 = RaftServer::new(1);
        raft1.current_term = 5;

        let req = raft.append_entries(&1);
        assert_eq!(req, Ok(MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: Vec::new(),
            leader_commit: 0,
        }));
        let res = raft1.handle_append_entries_request(&0, &req.unwrap());
        raft.handle_append_entries_response(&1, &res);
        assert_eq!(raft.state, RaftState::Leader(LeaderState {
            next_index: [(1, 1)].iter().cloned().collect(),
            match_index: [(1, 0)].iter().cloned().collect(),
        }));
    }

    // TODO: ensure we follow heartbeat behaviour (§5.2).
    #[test]
    fn test_raft_server_append_entries_heartbeat() {
        let mut raft = RaftServer::new(0);
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 4),
        ]);
        raft.current_term = 5;
        raft.state = RaftState::Leader(LeaderState {
            next_index: [(0, 4), (1, 4), (2, 4)].iter().cloned().collect(),
            match_index: [(0, 0), (1, 0), (2, 0)].iter().cloned().collect(),
        });

        assert_eq!(raft.append_entries(&1), Ok(MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 3,
            prev_log_term: 4,
            entries: Vec::new(),
            leader_commit: 0,
        }));
    }

    // TODO: validate that the ID is also in the list of available peers.
    #[test]
    fn test_raft_server_append_entries_invalid_peer() {
        let raft = RaftServer::new(0);

        // Peer `id` is the same ID as the issuing server.
        assert_eq!(raft.append_entries(&0), Err(AppendEntriesError::InvalidPeer));
    }

    #[test]
    fn test_raft_server_append_entries_not_leader() {
        let raft = RaftServer::new(0);

        // server `state` is not leader.
        assert_eq!(raft.append_entries(&1), Err(AppendEntriesError::NotLeader));
    }

    /* `RaftServer::become_leader` tests */

    #[test]
    fn test_raft_server_become_leader() {
        let servers = [0, 1, 2, 3, 4].iter().cloned().collect();

        let mut raft = RaftServer::new(0);
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
        let mut raft = RaftServer::new(0);

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
        let mut raft = RaftServer::new(0);

        // `state` is `Follower`
        raft.state = RaftState::Follower;
        raft.become_leader(&HashSet::new());
    }

    /* `RaftServer::client_request` tests */

    #[test]
    fn test_raft_server_client_request() {
        let mut raft = RaftServer::new(0);
        raft.current_term = 3;
        raft.state = RaftState::Leader(LeaderState {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        });

        assert_eq!(raft.client_request(()), Ok(()));
        assert_eq!(raft.log, vec![LogEntry((), 3)]);
    }

    #[test]
    fn test_raft_server_client_request_not_leader() {
        let mut raft = RaftServer::new(0);

        // `state` is `Follower`
        raft.state = RaftState::Follower;
        assert_eq!(raft.client_request(()), Err(ClientRequestError::NotLeader));
    }

    /* `RaftServer::advance_commit_index` tests */

    #[test]
    fn test_raft_server_advance_commit_index() {
        let servers = [0, 1, 2, 3, 4].iter().cloned().collect();
        let mut raft = RaftServer::new(0);
        raft.commit_index = 1;
        raft.current_term = 3;
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 4),
            LogEntry((), 5),
        ]);
        raft.state = RaftState::Leader(LeaderState {
            next_index: HashMap::new(),
            match_index: [(1, 5), (2, 3), (3, 3), (4, 0)].iter().cloned().collect(),
        });

        assert_eq!(raft.advance_commit_index(&servers), Ok(()));
        assert_eq!(raft.commit_index, 3);
    }

    #[test]
    fn test_raft_server_advance_commit_index_not_matching_term() {
        let servers = [0, 1, 2, 3, 4].iter().cloned().collect();
        let mut raft = RaftServer::new(0);
        raft.commit_index = 1;
        // Majority of servers agree on index 3, but `log[3].term !=
        // self.current_term`.
        raft.current_term = 2;
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 4),
            LogEntry((), 5),
        ]);
        raft.state = RaftState::Leader(LeaderState {
            next_index: HashMap::new(),
            match_index: [(1, 5), (2, 3), (3, 3), (4, 0)].iter().cloned().collect(),
        });

        assert_eq!(raft.advance_commit_index(&servers), Ok(()));
        assert_eq!(raft.commit_index, 1);
    }

    #[test]
    fn test_raft_server_advance_commit_index_not_leader() {
        let servers = [0, 1, 2, 3, 4].iter().cloned().collect();
        let mut raft = RaftServer::new(0);

        // `state` is `Follower`
        raft.state = RaftState::Follower;
        assert_eq!(raft.advance_commit_index(&servers), Err(AdvanceCommitIndexError::NotLeader));
    }

    #[test]
    fn test_raft_server_advance_commit_index_no_quorum() {
        let servers = [0, 1, 2, 3, 4].iter().cloned().collect();
        let mut raft = RaftServer::new(0);
        raft.commit_index = 1;
        raft.state = RaftState::Leader(LeaderState {
            next_index: HashMap::new(),
            // Majority of servers don't agree on any index > 0.
            match_index: [(1, 5), (2, 3), (3, 0), (4, 0)].iter().cloned().collect(),
        });

        assert_eq!(raft.advance_commit_index(&servers), Ok(()));
        assert_eq!(raft.commit_index, 1);
    }

    /* AppendEntries RPC tests */

    // Reply false if `term` < `current_term` (§5.1).
    #[test]
    fn test_raft_server_handle_append_entries_request_reply_false_when_term_less_than_current_term() {
        let ae = MessageAppendEntries {
            term: 1,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        let mut raft = RaftServer::new(0);

        // `current_term` is higher than `term`
        raft.set_current_term(5);
        let aer = raft.handle_append_entries_request(&1, &ae);
        assert_eq!(aer.term, 5);
        assert_eq!(aer.success, false);
    }

    // Reply false if log doesn't contain an entry at `prev_log_index` whose
    // term matches `prev_log_term` (§5.3).
    #[test]
    fn test_raft_server_handle_append_entries_request_reply_false_when_log_doesnt_contain_term_matching_prev_log_term() {
        let ae = MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 4,
            entries: Vec::new(),
            leader_commit: 0,
        };

        let mut raft = RaftServer::new(0);
        raft.set_current_term(5);

        // `log` does not contain an entry at `prev_log_index`
        let aer = raft.handle_append_entries_request(&1, &ae);

        assert_eq!(aer.success, false);

        // `log` does not contain an entry at `prev_log_index` whose term
        // matches `prev_log_term`
        // TODO: add these entries via an RPC call instead.
        raft.log.push(LogEntry((), 3));
        let aer = raft.handle_append_entries_request(&1, &ae);

        assert_eq!(aer.success, false);
    }

    // If an existing entry conflicts with a new one (same index but different
    // terms), delete the existing entry and all that follow it (§5.3).
    #[test]
    fn test_raft_server_handle_append_entries_request_existing_entry_conflicts_with_new_one() {
        let ae = MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 2,
            prev_log_term: 2,
            entries: vec![LogEntry((), 4)],
            leader_commit: 0,
        };

        let mut raft = RaftServer::new(0);

        // Existing entry conflicts with a new one.
        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 3),
        ]);
        raft.set_current_term(5);
        let aer = raft.handle_append_entries_request(&1, &ae);

        // NOTE: although the conflicting entry and all that follow it should be
        // be removed eventually, each conflicting AppendEntries RPC call will
        // only remove one entry at a time.
        assert_eq!(aer.success, true);
        assert_eq!(raft.log, vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
        ]);
    }

    // New entry is already appended.
    #[test]
    fn test_raft_server_handle_append_entries_request_append_new_entries_already_done() {
        let ae = MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry((), 2), LogEntry((), 3), LogEntry((), 4)],
            leader_commit: 0,
        };

        let mut raft = RaftServer::new(0);

        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
        ]);
        raft.set_current_term(5);
        let aer = raft.handle_append_entries_request(&1, &ae);

        assert_eq!(aer.term, 5);
        assert_eq!(aer.success, true);
        assert_eq!(raft.log, vec![
            LogEntry((), 1),
            LogEntry((), 2),
        ]);
    }

    // Append any new entries not already in the log.
    #[test]
    fn test_raft_server_handle_append_entries_request_append_new_entries() {
        let ae = MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry((), 2), LogEntry((), 3), LogEntry((), 4)],
            leader_commit: 0,
        };

        let mut raft = RaftServer::new(0);

        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
        ]);
        raft.set_current_term(5);
        let aer = raft.handle_append_entries_request(&1, &ae);

        assert_eq!(aer.term, 5);
        assert_eq!(aer.success, true);
        assert_eq!(raft.log, vec![
            LogEntry((), 1),
            LogEntry((), 2),
        ]);
    }

    // If `leader_commit` > `commit_index`, set `commit_index =
    // min(leader_commit, index of last new entry)`.
    #[test]
    fn test_raft_server_handle_append_entries_request_leader_commit_greater_than_commit_index() {
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

        let mut raft = RaftServer::new(0);
        raft.set_current_term(5);

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
        raft.handle_append_entries_request(&1, &ae);

        assert_eq!(raft.commit_index, 3);

        // `leader_commit` is greater than `commit_index`, and greater than
        // index of last new entry
        raft.set_commit_index(2);
        ae.leader_commit = 7;
        raft.handle_append_entries_request(&1, &ae);

        assert_eq!(raft.commit_index, 4);;
    }

    /* AppendEntries RPC response tests */

    #[test]
    #[should_panic]
    fn test_raft_server_handle_append_entries_response_invalid_term() {
        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: true,
        };

        let mut raft = RaftServer::new(0);

        // `current_term` is not equal to `term`
        raft.set_current_term(0);
        raft.handle_append_entries_response(&1, &aer);
    }

    #[test]
    fn test_raft_server_handle_append_entries_response_success() {
        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: true,
        };

        let mut raft = RaftServer::new(0);
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

        raft.handle_append_entries_response(&1, &aer);
        assert_eq!(raft.state, RaftState::Leader(LeaderState {
            next_index: [(1, 8), (2, 7)].iter().cloned().collect(),
            match_index: [(1, 1), (2, 0)].iter().cloned().collect(),
        }));
    }

    #[test]
    fn test_raft_server_handle_append_entries_response_not_success() {
        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: false,
        };

        let mut raft = RaftServer::new(0);
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

        raft.handle_append_entries_response(&1, &aer);
        assert_eq!(raft.state, RaftState::Leader(LeaderState {
            next_index: [(1, 6), (2, 7)].iter().cloned().collect(),
            match_index: [(1, 0), (2, 0)].iter().cloned().collect(),
        }));
    }

    /* RequestVote RPC tests */

    // Reply false if `term` < `current_term` (§5.1).
    #[test]
    fn test_raft_server_handle_request_vote_request_reply_false_when_term_less_than_current_term() {
        let rv = MessageRequestVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 5,
            last_log_term: 5,
        };

        let mut raft = RaftServer::new(0);

        // `current_term` is higher than `term`
        raft.set_current_term(5);
        let rvr = raft.handle_request_vote_request(&1, &rv);
        assert_eq!(rvr.term, 5);
        assert_eq!(rvr.vote_granted, false);
        assert_eq!(raft.voted_for, None);
    }

    /*
     * If `voted_for` is `None` or `candidate_id`, and candidate's log is at
     * least as up-to-date as receiver's log, grant vote (§5.2, §5.4).
     */

    #[test]
    fn test_raft_server_handle_request_vote_request_reply_false_when_voted_for_is_not_candidate_id() {
        let rv = MessageRequestVote {
            term: 5,
            candidate_id: 1,
            last_log_index: 5,
            last_log_term: 5,
        };

        let mut raft = RaftServer::new(0);

        // `voted_for` does not match `candidate_id`
        raft.voted_for = Some(3);
        raft.current_term = 5;
        let rvr = raft.handle_request_vote_request(&1, &rv);
        assert_eq!(rvr.term, 5);
        assert_eq!(rvr.vote_granted, false);
        assert_eq!(raft.voted_for, Some(3));
    }

    #[test]
    fn test_raft_server_handle_request_vote_request_reply_false_if_candidates_log_is_not_up_to_date() {
        let rv = MessageRequestVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 3,
            last_log_term: 3,
        };

        let mut raft = RaftServer::new(0);

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
        let rvr = raft.handle_request_vote_request(&1, &rv);
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
        let rvr = raft.handle_request_vote_request(&1, &rv);
        assert_eq!(rvr.term, 1);
        assert_eq!(rvr.vote_granted, false);
        assert_eq!(raft.voted_for, None);
    }

    #[test]
    fn test_raft_server_handle_request_vote_request_grant_vote() {
        let rv = MessageRequestVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 3,
            last_log_term: 3,
        };

        let mut raft = RaftServer::new(0);

        // TODO: replace this with a call to `update_term`.
        raft.current_term = 1;
        // TODO: add these entries via an RPC call instead.
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
        ]);
        let rvr = raft.handle_request_vote_request(&1, &rv);
        assert_eq!(rvr.term, 1);
        assert_eq!(rvr.vote_granted, true);
        assert_eq!(raft.voted_for, Some(1));
    }

    /* RequestVote RPC response tests */

    #[test]
    #[should_panic]
    fn test_raft_server_handle_request_vote_response_invalid_term() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: true,
        };

        let mut raft = RaftServer::new(0);

        // `current_term` is not equal to `term`
        raft.set_current_term(0);
        raft.handle_request_vote_response(&1, &rvr);
    }

    #[test]
    fn test_raft_server_handle_request_vote_response_vote_granted() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: true,
        };

        let mut raft = RaftServer::new(0);
        // TODO: replace this with a call to `update_term`.
        raft.current_term = 1;
        raft.state = RaftState::Candidate(CandidateState::default());

        assert_eq!(rvr.term, raft.current_term);
        raft.handle_request_vote_response(&1, &rvr);
        assert_eq!(raft.state,
            RaftState::Candidate(CandidateState {
                votes_responded: [1].iter().cloned().collect(),
                votes_granted: [1].iter().cloned().collect(),
            })
        );
    }

    #[test]
    fn test_raft_server_handle_request_vote_response_vote_not_granted() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: false,
        };

        let mut raft = RaftServer::new(0);
        // TODO: replace this with a call to `update_term`.
        raft.current_term = 1;
        raft.state = RaftState::Candidate(CandidateState::default());

        assert_eq!(rvr.term, raft.current_term);
        raft.handle_request_vote_response(&1, &rvr);
        assert_eq!(raft.state,
            RaftState::Candidate(CandidateState {
                votes_responded: [1].iter().cloned().collect(),
                ..Default::default()
            })
        );
    }

    /* RaftServer::update_term tests */

    #[test]
    #[should_panic(expected = "assertion failed: msg.term() > self.current_term")]
    fn test_raft_server_update_term_older_term() {
        let rvr = Message::RequestVoteResponse(MessageRequestVoteResponse {
            term: 1,
            vote_granted: false,
        });

        let mut raft = RaftServer::new(0);
        // `current_term` is greater than `term`.
        raft.current_term = 2;

        raft.update_term(&1, &rvr);
    }

    #[test]
    fn test_raft_server_update_term_newer_term() {
        let rvr = Message::RequestVoteResponse(MessageRequestVoteResponse {
            term: 2,
            vote_granted: false,
        });

        let mut raft = RaftServer::new(0);
        // `current_term` is less than `term`.
        raft.current_term = 1;
        raft.state = RaftState::Leader(LeaderState {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        });
        raft.voted_for = Some(1);

        raft.update_term(&1, &rvr);
        assert_eq!(raft.current_term, 2);
        assert_eq!(raft.state, RaftState::Follower);
        assert_eq!(raft.voted_for, None);
    }

    /* RaftServer::receive */

    // Any RPC with a newer term causes the recipient to advance its term first.
    // Responses with stale terms are ignored.
    #[test]
    fn test_raft_server_receive_message_update_term() {
        let rvr = Message::RequestVoteResponse(MessageRequestVoteResponse {
            term: 2,
            vote_granted: false,
        });

        let mut raft = RaftServer::new(0);
        raft.state = RaftState::Candidate(CandidateState::default());
        // `current_term` is less than `term`.
        raft.current_term = 1;

        assert_eq!(raft.receive(&1, &rvr), ReceiveResult::UpdatedTerm);
        assert_eq!(raft.state, RaftState::Follower);
        assert_eq!(raft.current_term, 2);
    }

    #[test]
    fn test_raft_server_receive_message_processed() {
        let rvr = Message::RequestVoteResponse(MessageRequestVoteResponse {
            term: 2,
            vote_granted: false,
        });

        let mut raft = RaftServer::new(0);
        raft.state = RaftState::Candidate(CandidateState::default());
        raft.current_term = 2;

        assert_eq!(raft.receive(&1, &rvr), ReceiveResult::ResponseProcessed);
        assert_eq!(raft.state, RaftState::Candidate(CandidateState {
            votes_responded: [1].iter().cloned().collect(),
            ..Default::default()
        }));
    }

    #[test]
    fn test_raft_server_receive_message_stale() {
        let rvr = Message::RequestVoteResponse(MessageRequestVoteResponse {
            term: 1,
            vote_granted: false,
        });

        let mut raft = RaftServer::new(0);
        raft.state = RaftState::Candidate(CandidateState::default());
        // `current_term` is greater than `term`.
        raft.current_term = 2;

        assert_eq!(raft.receive(&1, &rvr), ReceiveResult::DropStaleResponse);
        assert_eq!(raft.state, RaftState::Candidate(CandidateState::default()));
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

    #[test]
    fn test_sub_seq() {
        assert_eq!(sub_seq(&[0, 1, 2], 0, 0), &[0]);
        assert_eq!(sub_seq(&[0, 1, 2], 0, 10), &[0, 1, 2]);
        assert_eq!(sub_seq(&[0, 1, 2], 1, 0), &[]);
        assert_eq!(sub_seq(&[0, 1, 2], 5, 0), &[]);
    }
}
