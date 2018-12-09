#![allow(dead_code, unused_variables)]

use libc::c_void;
use rand::{self, Rng};
use std::cmp::{self, Ordering};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ptr;
use utils::CVec;

// Randomized election timeout range, in milliseconds.
const ELECTION_TIMEOUT_RANGE: (usize, usize) = (150, 300);
// We set the heartbeat interval to half of the minimum election timeout, so
// that the smallest possible downtime is about half of the minimum election
// timeout.
const HEARTBEAT_INTERVAL: usize = ELECTION_TIMEOUT_RANGE.0 / 2;

pub type UserData = *mut c_void;

pub type Id = usize;
type Command = ();
pub type Term = usize;
/// An individual log entry.
///
/// Each entry contains a command for the state machine, and the term when the
/// entry was received by the leader (first index is 1).
#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry(Command, Term);
/// Log to replicate across servers.
type Log = Vec<LogEntry>;

impl LogEntry {
    fn term(&self) -> Term {
        self.1
    }
}

/// Message invoked by leader to replicate log entries (§5.3); also used as
/// heartbeat (§5.2).
#[derive(Clone, Debug, PartialEq)]
pub struct MessageAppendEntries {
    /// Leader's term.
    pub term: Term,
    /// Used so follower nodes can redirect clients to the leader.
    pub leader_id: Id,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: usize,
    /// Term of previous `prev_log_index` entry.
    pub prev_log_term: Term,
    /// Log entries to store.
    ///
    /// Empty for heartbeat messages; may send more than one for efficiency.
    pub entries: Vec<LogEntry>,
    /// Leader's `commit_index`.
    pub leader_commit: usize,
}

impl From<MessageAppendEntriesRaw> for MessageAppendEntries {
    fn from(msg: MessageAppendEntriesRaw) -> Self {
        Self {
            term: msg.term,
            leader_id: msg.leader_id,
            prev_log_index: msg.prev_log_index,
            prev_log_term: msg.prev_log_term,
            entries: (*CVec::new(msg.entries_ptr, msg.entries_num)).to_vec(),
            leader_commit: msg.leader_commit,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[repr(C)]
pub struct MessageAppendEntriesRaw {
    pub term: Term,
    pub leader_id: Id,
    pub prev_log_index: usize,
    pub prev_log_term: Term,
    pub entries_num: usize,
    pub entries_ptr: *const LogEntry,
    pub leader_commit: usize,
}

impl From<MessageAppendEntries> for MessageAppendEntriesRaw {
    fn from(msg: MessageAppendEntries) -> Self {
        Self {
            term: msg.term,
            leader_id: msg.leader_id,
            prev_log_index: msg.prev_log_index,
            prev_log_term: msg.prev_log_term,
            entries_num: msg.entries.len(),
            entries_ptr: msg.entries.as_ptr(),
            leader_commit: msg.leader_commit,
        }
    }
}

/// Message issued in response to an `AppendEntries` RPC.
#[derive(Clone, Debug, PartialEq)]
#[repr(C)]
pub struct MessageAppendEntriesResponse {
    /// `current_term`, for leader to update itself.
    pub term: Term,
    /// `true` if follower contained entry matching `prev_log_index` and
    /// `prev_log_term`.
    pub success: bool,

    /// Index of the last appended entry if the RPC was successful;
    /// `current_index` otherwise.
    // TODO: determine if this does not violate the Raft specification.
    pub current_index: usize,
}

/// Message invoked by candidates to gather votes (§5.2).
#[derive(Clone, Debug, PartialEq)]
#[repr(C)]
pub struct MessageRequestVote {
    /// Candidate's term.
    pub term: Term,
    /// Candidate requesting vote.
    pub candidate_id: Id,
    /// Index of candidate's last log entry.
    pub last_log_index: usize,
    /// Term of candidate's last log entry.
    pub last_log_term: usize,
}

/// Message issued in response to a `RequestVote` RPC.
#[derive(Clone, Debug, PartialEq)]
#[repr(C)]
pub struct MessageRequestVoteResponse {
    /// `current_term`, for candidate to update itself.
    pub term: Term,
    /// `true` means candidate received vote.
    pub vote_granted: bool,
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

    /// Add a received vote to the candidate's state.
    ///
    /// # Invariants
    ///
    /// 1. Should only be called if `peer` has not already responded during the
    ///    current election.
    // TODO: is the above invariant necessary?
    fn receive_vote(&mut self, peer: Id, granted: bool) {
        let not_already_responded = self.votes_responded.insert(peer);
        assert!(not_already_responded);
        if granted {
            let not_already_granted = self.votes_granted.insert(peer);
            assert!(not_already_granted);
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

impl LeaderState {
    /// Initialize a new `LeaderState`.
    fn new(next_index: HashMap<Id, usize>, match_index: HashMap<Id, usize>) -> Self {
        for (_, idx) in &next_index {
            // Logs are 1-indexed, thus cannot be zero.
            assert!(*idx > 0);
        }

        Self {
            next_index,
            match_index,
        }
    }

    fn next_index(&self, id: &Id) -> Option<usize> {
        self.next_index.get(id).map(ToOwned::to_owned)
    }

    fn set_next_index(&mut self, id: Id, next_index: usize) {
        // Logs are 1-indexed, thus cannot be zero.
        assert!(next_index > 0);
        self.next_index.insert(id, next_index);
    }
}

impl Default for LeaderState {
    fn default() -> Self {
        Self::new(HashMap::new(), HashMap::new())
    }
}

/// Errors which may occur when attempting to send a RequestVote request.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestVoteError {
    /// A RequestVote request may not be issued to a server which has already
    /// responded to a RequestVote request.
    AlreadyResponded,
    /// A non-candidate node cannot issue a RequestVote request.
    NotCandidate,
}

/// Errors which may occur when attempting to send an AppendEntries request.
#[derive(Clone, Debug, PartialEq)]
pub enum AppendEntriesError {
    /// Attempted to send an AppendEntries request to oneself or an unknown
    /// peer.
    InvalidPeer,
    /// A server must be a leader to send an AppendEntries request.
    NotLeader,
}

/// Errors which may occur when receiving a client request to add a value to the
/// log.
#[derive(Clone, Debug, PartialEq)]
enum ClientRequestError {
    /// A non-leader node received a client request.
    NotLeader,
}

/// Errors which may occur when issuing an election timeout for the server.
#[derive(Clone, Debug, PartialEq)]
pub enum TimeoutError {
    /// A leader may not experience an election timeout.
    IsLeader,
}

/// Errors which may occur during Raft server operation.
#[derive(Clone, Debug, PartialEq)]
pub enum RaftServerError {
    /// A required callback function was unregistered.
    UnregisteredCallbackFn,
    /// An error occurred while constructing an AppendEntries RPC request.
    AppendEntriesError(AppendEntriesError),
}

impl From<AppendEntriesError> for RaftServerError {
    fn from(error: AppendEntriesError) -> Self {
        RaftServerError::AppendEntriesError(error)
    }
}

/// Errors which may occur when advancing the commit index for a leader node.
#[derive(Clone, Debug, PartialEq)]
enum AdvanceCommitIndexError {
    /// A non-leader node may not advance its commit index.
    NotLeader,
}

/// Errors which may occur when attempting to transition from the candidate to
/// the leader state.
#[derive(Clone, Debug, PartialEq)]
pub enum BecomeLeaderError {
    /// The node has insufficient votes to form a quorum.
    InsufficientVotesForQuorum,
}

pub type SendAppendEntriesExtern = extern fn(raft: *mut RaftServer, user_data: *mut c_void, peer_id: Id, msg: *mut MessageAppendEntriesRaw);
pub type SendRequestVoteExtern = extern fn(raft: *mut RaftServer, user_data: *mut c_void, peer_id: Id, msg: *mut MessageRequestVote);

/// Callbacks used by the Raft server.
#[repr(C)]
#[derive(Clone, Debug, PartialEq)]
pub struct Callbacks {
    /// Callback to send a `MessageAppendEntries` to a peer.
    pub send_append_entries: SendAppendEntriesExtern,
    /// Callback to send a `MessageRequestVote` to a peer.
    pub send_request_vote: SendRequestVoteExtern,
}

#[derive(Clone, Debug, PartialEq)]
enum RaftState {
    Candidate(CandidateState),
    Follower,
    Leader(LeaderState),
}

/// State shared between both remote Raft peers and the local Raft server.
#[derive(Clone, Debug)]
#[repr(C)]
pub struct NodeInfo {
    /// ID of the Raft node.
    pub id: Id,
    /// Extra data added for FFI purposes.
    pub user_data: UserData,
}

/// Raft server implementation.
///
/// # Panics
///
/// * Panics if the Raft server is in operation and callbacks have not been
///   registered with `RaftServer::register_callbacks`
#[derive(Clone, Debug)]
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

    /// Raft state of this server (e.g. candidate/follower/leader).
    state: RaftState,

    /// Elapsed time (in ms) since the current election began.
    election_timer: usize,
    /// Randomized election timeout.
    election_timeout: usize,

    /// Callbacks used by the Raft server.
    callbacks: Option<Callbacks>,

    /// Node information for this Raft server.
    node: NodeInfo,
    /// Node information for remote Raft peers.
    peers: HashMap<Id, NodeInfo>,
}

impl RaftServer {
    /// Instantiate a new Raft server.
    pub fn new(id: Id) -> Self {
        let mut raft = Self {
            current_term: 0,
            voted_for: None,
            log: log_new(),
            commit_index: 0,
            last_applied: 0,
            state: RaftState::Follower,
            election_timer: 0,
            election_timeout: 0,
            callbacks: None,
            node: NodeInfo {
                id,
                user_data: ptr::null_mut(),
            },
            peers: HashMap::new(),
        };
        raft.randomize_election_timeout();
        raft
    }

    pub fn voted_for(&self) -> Option<Id> {
        self.voted_for
    }

    pub fn id(&self) -> Id {
        self.node.id
    }

    fn set_current_term(&mut self, term: usize) {
        self.current_term = term;
    }

    fn set_voted_for(&mut self, id: Option<Id>) {
        println!("RAFT: server {} voted for: {:?}", self.id(), id);
        self.voted_for = id;
    }

    fn set_commit_index(&mut self, index: usize) {
        self.commit_index = index;
    }

    fn set_state(&mut self, state: RaftState) {
        self.state = state;
    }

    /// Return a list of every peer's ID.
    fn peer_ids(&self) -> HashSet<Id> {
        self.peers.keys().map(ToOwned::to_owned).collect::<HashSet<Id>>()
    }

    /// Return the a list of every node's ID, including this node.
    fn node_ids(&self) -> HashSet<Id> {
        let mut ids = self.peer_ids();
        ids.insert(self.id());
        ids
    }

    pub fn add_peer(&mut self, id: Id, user_data: UserData) {
        // A server cannot have itself as a peer.
        assert_ne!(self.id(), id);
        self.peers.insert(id, NodeInfo {
            id,
            user_data,
        });
    }

    /// Return the node info for the node associated with the specified `Id`.
    pub fn get_node_info_by_id(&self, id: Id) -> Option<&NodeInfo> {
        if id == self.id() {
            Some(&self.node)
        } else {
            self.peers.get(&id)
        }
    }

    pub fn register_callbacks(&mut self, callbacks: Callbacks, user_data: *mut c_void) {
        self.callbacks = Some(callbacks);
        self.node.user_data = user_data;
    }

    /// Randomize the election timeout to be anywhere in the `ELECTION_TIMEOUT_RANGE`, inclusive.
    fn randomize_election_timeout(&mut self) {
        self.election_timeout = rand::thread_rng().gen_range(ELECTION_TIMEOUT_RANGE.0, ELECTION_TIMEOUT_RANGE.1 + 1);
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
            candidate_id: self.id(),
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
        if &self.id() == peer {
            return Err(AppendEntriesError::InvalidPeer);
        }

        if !self.is_leader() {
            return Err(AppendEntriesError::NotLeader);
        }

        match self.state {
            RaftState::Leader(ref state) => {
                let next_index = match state.next_index(peer) {
                    Some(next_index) => next_index,
                    None => panic!("Leader state missing next_index for peer {}", peer),
                };
                println!("next_index: {}", next_index);
                let prev_log_index = next_index - 1;
                let prev_log_term = if prev_log_index > 0 {
                    match self.log.get(prev_log_index - 1) {
                        Some(entry) => entry.term(),
                        None => panic!("Missing log entry: {}", prev_log_index),
                    }
                } else {
                    0
                };

                // Send up to 1 entry, constrained by the end of the log.
                let last_entry = cmp::min(self.log.len(), next_index);
                // TODO: determine if `sub_seq` is really necessary here,
                // assuming that `next_index` and `last_entry` have sane
                // values.
                // It should be possible to do this with simple slicing.
                let entries = sub_seq(&self.log, next_index, last_entry).to_vec();

                return Ok(MessageAppendEntries {
                    term: self.current_term,
                    leader_id: self.id(),
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: cmp::min(self.commit_index, last_entry),
                });
            },
            _ => unreachable!(),
        }
    }

    /// Send an AppendEntries RPC to a single peer.
    fn send_append_entries(&mut self, peer_id: Id) -> Result<(), RaftServerError> {
        let f = {
            let callbacks = self.callbacks
                .as_ref()
                .ok_or_else(|| RaftServerError::UnregisteredCallbackFn)?;
            callbacks.send_append_entries
        };
        let req = &mut { self.append_entries(&peer_id)?.into() };
        (f)(self, self.node.user_data, peer_id, req as *mut _);
        Ok(())
    }

    /// Node transitions to candidate.
    ///
    /// On conversion to candidate, start election.
    ///
    /// # Panics
    ///
    /// Panics if the server is currently in the candidate state.
    fn become_candidate(&mut self) -> Result<(), RaftServerError> {
        if self.is_candidate() {
            panic!("become_candidate called on candidate node");
        }

        self.state = RaftState::Candidate(CandidateState::default());

        self.start_election()
    }

    /// Attempt to transition this candidate node to leader.
    /// Returns `true` if the candidate became the leader.
    ///
    /// # Panics
    ///
    /// Panics if the server is not currently in the candidate state.
    pub fn try_become_leader(&mut self) -> Result<bool, RaftServerError> {
        {
            let state = match self.state {
                RaftState::Candidate(ref state) => state,
                _ => panic!("try_become_leader called on non-candidate node"),
            };

            if !is_quorum(&state.votes_granted, &self.node_ids()) {
                return Ok(false);
            }
        }

        self.state = RaftState::Leader(LeaderState::new(
            self.node_ids().iter().cloned().map(|s| (s, self.log.len() + 1)).collect(),
            self.node_ids().iter().cloned().map(|s| (s, 0)).collect(),
        ));

        // Upon election: send initial empty AppendEntries RPCs (heartbeat) to
        // each server; repeat during idle periods to prevent election timeouts
        // (§5.2).
        assert!(self.is_leader());
        println!("RAFT: server {} became leader", self.id());
        for id in self.peer_ids() {
            self.send_append_entries(id)?;
        }

        Ok(true)
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
    fn advance_commit_index(&mut self) -> Result<(), AdvanceCommitIndexError> {
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
                        .filter(|(&id, &m)| id == self.id() || m >= idx)
                        .map(|(&id, _)| id)
                        .collect();
                    is_quorum(&agree_servers, &self.node_ids())
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

    // FIXME: update this documentation to reflect the fact that term updating
    // is now handled as part of this function.
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
    /// 1. This function must never be called for a leader node.
    ///
    /// # Panics
    ///
    /// Panics if any invariants are violated.
    // TODO: ensure that only `msg.entries` of length `0` or `1` are handled.
    pub fn handle_append_entries_request(&mut self, peer: &Id, msg: &MessageAppendEntries) -> MessageAppendEntriesResponse {
        // If the node is ahead of us, update our term.
        if msg.term > self.current_term {
            self.update_term(peer, msg.term);
        }

        // Assert the invariants.
        assert!(msg.term <= self.current_term);
        // Non-leaders should not be issuing AppendEntries RPC calls.
        assert!(!self.is_leader());

        // If `msg.term < self.current_term` we reject the request.
        if msg.term < self.current_term {
            return MessageAppendEntriesResponse {
                term: self.current_term,
                success: false,
                current_index: self.log.len(),
            };
        }

        assert_eq!(msg.term, self.current_term);

        // If we're a candidate, we should return to the follower state.
        if self.is_candidate() {
            self.state = RaftState::Follower;
            return MessageAppendEntriesResponse {
                term: self.current_term,
                success: false,
                current_index: self.log.len(),
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
                                current_index: self.log.len(),
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
                current_index: self.log.len(),
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
                current_index: self.log.len(),
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
                current_index: self.log.len(),
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
                    current_index: self.log.len(),
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
            current_index: self.log.len(),
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
    pub fn handle_append_entries_response(&mut self, peer: &Id, msg: &MessageAppendEntriesResponse) -> Result<(), RaftServerError> {
        assert!(self.is_leader());

        // If the node is ahead of us, update our term, then ignore the response
        // as we are no longer the leader.
        if msg.term > self.current_term {
            self.update_term(peer, msg.term);
            return Ok(());
        }

        // Responses with stale items are ignored.
        if msg.term < self.current_term {
            return Ok(());
        }

        // `term` should be equal to `current_term` as the server issuing the
        // response should've increased its term to match that of the issued
        // request, and as the request came from this server, it should match
        // `current_term`.
        assert!(msg.term == self.current_term);

        match self.state {
            RaftState::Leader(ref mut state) => {
                match state.next_index(peer) {
                    Some(next_index) => {
                        // `match_index` only needs to be modified if the
                        // RPC call was successful; otherwise, we retry the RPC.
                        if msg.success {
                            // FIXME: make sure this works
                            //state.set_next_index(*peer, next_index + 1);
                            state.set_next_index(*peer, msg.current_index + 1);

                            match state.match_index.get_mut(peer) {
                                Some(match_index) => {
                                    // FIXME: make sure this works.
                                    //*match_index = *match_index + 1;
                                    *match_index = msg.current_index + 1;
                                },
                                None => panic!("Leader state missing match_index for peer {}", peer),
                            };

                            return Ok(());
                        } else {
                            // FIXME: make sure this works & get rid of the
                            // unwrap below.
                            //state.set_next_index(*peer, cmp::max(next_index - 1, 1));
                            if msg.current_index < *state.match_index.get(peer).unwrap() {}
                            else if msg.current_index < next_index - 1 {
                                state.set_next_index(*peer, cmp::min(msg.current_index + 1, self.log.len()));
                            } else {
                                state.set_next_index(*peer, next_index - 1);
                            }
                        }
                    },
                    None => panic!("Leader state missing next_index for peer {}", peer),
                };
            },
            _ => panic!("Non-leader node handling AppendEntries response"),
        }

        self.send_append_entries(*peer)
    }

    /// Server receives a RequestVote request from server `peer`.
    pub fn handle_request_vote_request(&mut self, peer: &Id, msg: &MessageRequestVote) -> MessageRequestVoteResponse {
        // If the node is ahead of us, update our term.
        if msg.term > self.current_term {
            self.update_term(peer, msg.term);
        }

        // `term` should never be greater than `current_term` as  `current_term`
        // should've been advanced after receiving the RPC.
        assert!(msg.term <= self.current_term);

        if msg.term < self.current_term {
            return MessageRequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        match self.voted_for {
            Some(candidate_id) if candidate_id == msg.candidate_id => {},
            None => {},
            _ => return MessageRequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            }
        }

        // Determine if the candidate's or the receiver's log is more up-to-date.
        // The more up-to-date of the two logs is the log which:
        // 1. has the later term, if the last entries in the logs have different
        //    terms; otherwise,
        // 2. whichever log is longer is more up-to-date.
        match msg.last_log_term.cmp(&last_term(&self.log)) {
            Ordering::Less => {
                return MessageRequestVoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                };
            },
            Ordering::Equal => if msg.last_log_index < self.log.len() {
                return MessageRequestVoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                };
            },
            _ => {},
        }

        // Update the candidate that was voted for.
        self.set_voted_for(Some(msg.candidate_id));

        return MessageRequestVoteResponse {
            term: self.current_term,
            vote_granted: true,
        };
    }

    /// Server receives a RequestVote response from server `peer`.
    pub fn handle_request_vote_response(&mut self, peer: &Id, msg: &MessageRequestVoteResponse) {
        // If the node is ahead of us, update our term, then ignore the response
        // as we are no longer a candidate.
        if msg.term > self.current_term {
            self.update_term(peer, msg.term);
            return;
        }

        // Responses with stale items are ignored.
        if msg.term < self.current_term {
            return;
        }

        // `term` should be equal to `current_term` as the server issuing the
        // response should've increased its term to match that of the issued
        // request, and as the request came from this server, it should match
        // `current_term`.
        assert!(msg.term == self.current_term);

        // Mark `peer` as having responded to our RequestVote RPC.
        if let RaftState::Candidate(ref mut state) = self.state {
            state.receive_vote(*peer, msg.vote_granted);
        }
    }

    /// Any RPC with a newer `term` causes the recipient to advance to its term
    /// first.
    /// Starts a new term with the specified value.
    ///
    /// # Panic
    ///
    /// Panics if the new term is not greater than the current term.
    fn update_term(&mut self, peer: &Id, term: Term) {
        // The term only needs to be updated if the RPC's `term` is greater than
        // the server's `current_term`.
        assert!(term > self.current_term);

        self.current_term = term;
        // Updating to a newer term puts the node into the follower state.
        self.state = RaftState::Follower;
        // Reset `voted_for` as we have not voted for anyone in the new term.
        self.voted_for = None;
    }

    /// Periodic function which runs election tasks.
    pub fn periodic(&mut self, ms_since_last_period: usize) -> Result<(), RaftServerError> {
        self.election_timer += ms_since_last_period;

        match self.state {
            // If a follower receives no communication over a period of time
            // called the *election timeout*, then it assumes there is no
            // viable leader and begins an election to choose a new leader.
            RaftState::Follower => {
                if self.is_election_timeout_elapsed() && self.voted_for().is_none() {
                    println!("RAFT: server {} becoming candidate", self.id());
                    self.become_candidate()?;
                }
            },
            RaftState::Candidate(_) => {
                if self.is_election_timeout_elapsed() {
                    println!("RAFT: server {} starting new election", self.id());
                    self.start_election()?;
                } else {
                    self.try_become_leader()?;
                }
            },
            RaftState::Leader(_) => {
                // TODO
                // If command received from client: append entry to local log,
                // respond after entry applied to state machine (§5.3).

                // TODO
                // If last log index >= nextIndex for a follower: send
                // AppendEntries RPC with log entries starting at nextIndex
                // (§5.3).

                // TODO
                // Upon election: send initial empty AppendEntries RPCs
                // (heartbeat) to each server; repeat during idle periods to
                // prevent election timeouts (§5.2).
                if self.is_heartbeat_interval_elapsed() {
                    for peer in self.peer_ids() {
                        self.send_append_entries(peer)?;
                    }
                }

                // TODO
                // If there exists an N such that N > commit_index, a majority
                // of match_index[i] >= N, and log[N].term == current_term:
                // set commit_index = N (§5.3, §5.4).
            },
        }

        Ok(())
    }

    /// Start a new election.
    ///
    /// To start an election:
    /// * Increment `current_term`
    /// * Vote for self
    /// * Reset election timer
    /// * Send RequestVote RPCs to all other servers
    ///
    /// # Invariants
    ///
    /// 1. Only candidates may start an election.
    /// 2. Callbacks must have been registered via `RaftServer::register_callbacks`.
    fn start_election(&mut self) -> Result<(), RaftServerError> {
        assert!(self.is_candidate());

        self.current_term += 1;

        let id = self.id();
        match self.state {
            RaftState::Candidate(ref mut state) => {
                // We first reset the election-related state, so that votes from
                // a previous election do not carry over.
                *state = CandidateState::default();

                // TODO: perhaps vote for self via a RequestVote RPC call
                // instead?
                state.receive_vote(id, true);
            },
            _ => unreachable!(),
        }
        // TODO: add tests to ensure that the server votes for itself.
        self.set_voted_for(Some(id));

        self.election_timer = 0;

        // Generate vote requests for each peer that has yet to vote.
        let mut vote_requests = self.peer_ids().clone().iter()
            .flat_map(|server| match self.request_vote(server) {
                Ok(msg) => Some((*server, msg)),
                // Per invariant #1, this should never be the case.
                Err(RequestVoteError::NotCandidate) => unreachable!(),
                // NOTE: in theory, this should be the case for only the present
                // server, as the election has just begun and no vote requests
                // have been issued yet.
                // However, we only iterate over the list of peers and not all
                // nodes, so we should never attempt to issue a request to
                // ourselves this way.
                Err(RequestVoteError::AlreadyResponded) => None,
            })
            .collect::<Vec<(Id, MessageRequestVote)>>();

        let f = {
            let callbacks = self.callbacks
                .as_ref()
                .ok_or_else(|| RaftServerError::UnregisteredCallbackFn)?;
            callbacks.send_request_vote
        };

        // Send the vote requests to each server.
        for (peer, req) in vote_requests.iter_mut() {
            (f)(self, self.node.user_data, *peer as Id, req as *mut _);
        }

        Ok(())
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

    /// Returns `true` if the randomized election timeout has elapsed.
    fn is_election_timeout_elapsed(&self) -> bool {
        self.election_timer >= self.election_timeout
    }

    /// Return `true` if the heartbeat interval has elapsed.
    fn is_heartbeat_interval_elapsed(&self) -> bool {
        self.election_timer >= HEARTBEAT_INTERVAL
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
    use std::cell::RefCell;
    use std::ptr;
    use super::*;

    #[derive(Debug, PartialEq)]
    enum Message {
        AppendEntries(MessageAppendEntriesRaw),
        RequestVote(MessageRequestVote),
    }

    thread_local!{
        static SENT_MESSAGES: RefCell<Vec<(usize, Message)>> = RefCell::new(Vec::new());
    }

    extern fn mock_send_append_entries(raft: *mut RaftServer, udata: *mut c_void, peer: usize, msg: *mut MessageAppendEntriesRaw) {
        let msg = unsafe {
            (*msg).clone()
        };
        SENT_MESSAGES.with(|prev| {
            (*prev.borrow_mut()).push((peer, Message::AppendEntries(msg)));
        });
    }
    extern fn mock_send_append_entries_noop(raft: *mut RaftServer, udata: *mut c_void, peer: usize, msg: *mut MessageAppendEntriesRaw) {}
    extern fn mock_send_request_vote(raft: *mut RaftServer, udata: *mut c_void, peer: usize, msg: *mut MessageRequestVote) {
        let msg = unsafe {
            (*msg).clone()
        };
        SENT_MESSAGES.with(|prev| {
            (*prev.borrow_mut()).push((peer, Message::RequestVote(msg)));
        });
    }
    extern fn mock_send_request_vote_noop(raft: *mut RaftServer, udata: *mut c_void, peer: usize, msg: *mut MessageRequestVote) {}

    const MOCK_CALLBACKS: Callbacks = Callbacks {
        send_append_entries: mock_send_append_entries,
        send_request_vote: mock_send_request_vote,
    };
    const MOCK_CALLBACKS_NOOP: Callbacks = Callbacks {
        send_append_entries: mock_send_append_entries_noop,
        send_request_vote: mock_send_request_vote_noop,
    };


    fn init_single_server() -> RaftServer {
        RaftServer::new(0)
    }

    #[test]
    fn test_candidate_state_new() {
        assert_eq!(CandidateState::default(), CandidateState {
            votes_responded: HashSet::new(),
            votes_granted: HashSet::new(),
        });
    }

    /* `CandidateState::receive_vote` tests */

    #[test]
    fn test_candidate_state_receive_vote() {
        let mut state = CandidateState::default();

        state.receive_vote(1, false);
        assert!(state.votes_responded.contains(&1));
        assert!(!state.votes_granted.contains(&1));
        state.receive_vote(2, true);
        assert!(state.votes_responded.contains(&2));
        assert!(state.votes_granted.contains(&2));
    }

    #[test]
    #[should_panic(expected = "assertion failed: not_already_responded")]
    fn test_candidate_state_receive_vote_already_responded() {
        let mut state = CandidateState {
            votes_responded: [1].iter().cloned().collect(),
            ..Default::default()
        };

        state.receive_vote(1, false);
    }

    #[test]
    fn test_raft_server_new() {
        let raft = init_single_server();
        // Term and index values MUST be initialized to 0 on first boot.
        assert_eq!(raft.current_term, 0);
        assert_eq!(raft.commit_index, 0);
        assert_eq!(raft.last_applied, 0);

        assert_eq!(raft.state, RaftState::Follower);
    }

    #[test]
    fn test_raft_server_set_current_term() {
        let mut raft = init_single_server();
        assert_eq!(raft.current_term, 0);
        raft.set_current_term(5);
        assert_eq!(raft.current_term, 5);
    }

    #[test]
    fn test_raft_server_set_state() {
        let mut raft = init_single_server();

        let leader_state = LeaderState::default();
        assert_eq!(raft.state, RaftState::Follower);
        raft.set_state(RaftState::Leader(leader_state.clone()));
        assert_eq!(raft.state, RaftState::Leader(leader_state));
    }

    /* `RaftServer::register_callbacks` tests */

    #[test]
    fn test_raft_server_register_callbacks() {
        let mut raft = init_single_server();

        raft.register_callbacks(MOCK_CALLBACKS_NOOP, ptr::null_mut());
    }

    #[test]
    fn test_raft_server_callback_send_request_vote() {
        let mut raft = init_single_server();
        raft.register_callbacks(MOCK_CALLBACKS, ptr::null_mut());

        let mut msg = MessageRequestVote {
            term: 1,
            candidate_id: 0,
            last_log_term: 1,
            last_log_index: 1,
        };
        SENT_MESSAGES.with(|messages| {
            assert_eq!(*messages, RefCell::new(Vec::new()));
            let f = {
                let callbacks = raft.callbacks.as_ref().unwrap();
                callbacks.send_request_vote
            };
            (f)(&mut raft, raft.node.user_data, 1, &mut msg as *mut _);
            assert_eq!(messages.borrow().get(0), Some(&(1, Message::RequestVote(msg))));
        });
    }

    /* `RaftServer::randomize_election_timeout` tests */

    #[test]
    fn test_raft_server_randomize_election_timeout() {
        let mut raft = init_single_server();
        let election_timeout = raft.election_timeout;

        raft.randomize_election_timeout();
        assert!(raft.election_timeout >= ELECTION_TIMEOUT_RANGE.0);
        assert!(raft.election_timeout <= ELECTION_TIMEOUT_RANGE.1);
    }


    /* `RaftServer::restart` tests */

    #[test]
    fn test_raft_server_restart() {
        let mut raft = RaftServer::new(1);
        raft.add_peer(0, ptr::null_mut());
        for peer in 2..4 {
            raft.add_peer(peer, ptr::null_mut());
        }
        raft.current_term = 1;
        raft.voted_for = Some(3);
        raft.log.append(&mut vec![LogEntry((), 1)]);
        raft.state = RaftState::Candidate(CandidateState::default());
        raft.commit_index = 2;

        raft.restart();
        assert_eq!(raft.node.id, 1);
        assert_eq!(raft.current_term, 1);
        assert_eq!(raft.voted_for, Some(3));
        assert_eq!(raft.log, vec![LogEntry((), 1)]);
        assert_eq!(raft.state, RaftState::Follower);
        assert_eq!(raft.commit_index, 0);
    }

    /* `RaftServer::timeout` tests */

    #[test]
    fn test_raft_server_timeout() {
        let mut raft = init_single_server();
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
        let mut raft = init_single_server();
        raft.state = RaftState::Leader(LeaderState::default());

        assert_eq!(raft.timeout(), Err(TimeoutError::IsLeader));
    }

    /* `RaftServer::request_vote` tests */

    #[test]
    fn test_raft_server_request_vote() {
        let mut raft = init_single_server();
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
        let mut raft = init_single_server();

        assert_eq!(raft.request_vote(&1), Err(RequestVoteError::NotCandidate));
    }

    #[test]
    fn test_raft_server_request_vote_already_voted() {
        let mut raft = init_single_server();
        raft.state = RaftState::Candidate(CandidateState {
            votes_responded: [1].iter().cloned().collect(),
            ..Default::default()
        });

        assert_eq!(raft.request_vote(&1), Err(RequestVoteError::AlreadyResponded));
    }

    /* `RaftServer::append_entries` tests */

    #[test]
    fn test_raft_server_append_entries_two_missing() {
        let servers: HashSet<Id> = [0, 1].iter().cloned().collect();
        let mut raft = RaftServer::new(0);
        raft.register_callbacks(MOCK_CALLBACKS_NOOP, ptr::null_mut());
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
        ]);
        raft.current_term = 5;
        raft.state = RaftState::Leader(LeaderState::new(
            [(1, 3)].iter().cloned().collect(),
            [(1, 0)].iter().cloned().collect(),
        ));

        let mut raft1 = RaftServer::new(1);
        raft1.current_term = 5;

        let res = {
            let req = raft.append_entries(&1);
            assert_eq!(req, Ok(MessageAppendEntries {
                term: 5,
                leader_id: 0,
                prev_log_index: 2,
                prev_log_term: 2,
                entries: vec![],
                leader_commit: 0,
            }));
            raft1.handle_append_entries_request(&0, &req.unwrap())
        };
        raft.handle_append_entries_response(&1, &res).unwrap();
        assert_eq!(raft.state, RaftState::Leader(LeaderState::new(
            [(1, 2)].iter().cloned().collect(),
            [(1, 0)].iter().cloned().collect(),
        )));
    }

    #[test]
    fn test_raft_server_append_entries_one_missing() {
        let servers: HashSet<Id> = [0, 1].iter().cloned().collect();
        let mut raft = RaftServer::new(0);
        raft.register_callbacks(MOCK_CALLBACKS_NOOP, ptr::null_mut());
        raft.log.append(&mut vec![
            LogEntry((), 1),
        ]);
        raft.current_term = 5;
        raft.state = RaftState::Leader(LeaderState::new(
            [(1, 2)].iter().cloned().collect(),
            [(1, 0)].iter().cloned().collect(),
        ));

        let mut raft1 = RaftServer::new(1);
        raft1.current_term = 5;

        let res = {
            let req = raft.append_entries(&1);
            assert_eq!(req, Ok(MessageAppendEntries {
                term: 5,
                leader_id: 0,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            }));
            raft1.handle_append_entries_request(&0, &req.unwrap())
        };
        raft.handle_append_entries_response(&1, &res).unwrap();
        assert_eq!(raft.state, RaftState::Leader(LeaderState::new(
            [(1, 1)].iter().cloned().collect(),
            [(1, 0)].iter().cloned().collect(),
        )));
    }

    // TODO: ensure we follow heartbeat behaviour (§5.2).
    #[test]
    fn test_raft_server_append_entries_heartbeat() {
        let mut raft = init_single_server();
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 4),
        ]);
        raft.current_term = 5;
        raft.state = RaftState::Leader(LeaderState::new(
            [(0, 4), (1, 4), (2, 4)].iter().cloned().collect(),
            [(0, 0), (1, 0), (2, 0)].iter().cloned().collect(),
        ));

        assert_eq!(raft.append_entries(&1), Ok(MessageAppendEntries {
            term: 5,
            leader_id: 0,
            prev_log_index: 3,
            prev_log_term: 4,
            entries: vec![],
            leader_commit: 0,
        }));
    }

    // TODO: validate that the ID is also in the list of available peers.
    #[test]
    fn test_raft_server_append_entries_invalid_peer() {
        let raft = init_single_server();

        // Peer `id` is the same ID as the issuing server.
        assert_eq!(raft.append_entries(&0), Err(AppendEntriesError::InvalidPeer));
    }

    #[test]
    fn test_raft_server_append_entries_not_leader() {
        let raft = init_single_server();

        // server `state` is not leader.
        assert_eq!(raft.append_entries(&1), Err(AppendEntriesError::NotLeader));
    }

    /* `RaftServer::become_candidate` tests */

    #[test]
    fn test_raft_server_become_candidate() {
        let mut raft = init_single_server();
        raft.register_callbacks(MOCK_CALLBACKS_NOOP, ptr::null_mut());

        raft.become_candidate().unwrap();
        assert_eq!(raft.state, RaftState::Candidate(CandidateState {
            votes_responded: [0].iter().cloned().collect(),
            votes_granted: [0].iter().cloned().collect(),
        }));
    }

    #[test]
    #[should_panic(expected = "become_candidate called on candidate node")]
    fn test_raft_server_become_candidate_already_candidate() {
        let mut raft = init_single_server();
        raft.state = RaftState::Candidate(CandidateState::default());

        raft.become_candidate().unwrap();
    }

    /* `RaftServer::try_become_leader` tests */

    #[test]
    fn test_raft_server_try_become_leader() {
        let mut raft = RaftServer::new(0);
        raft.register_callbacks(MOCK_CALLBACKS, ptr::null_mut());
        for peer in 1..5 {
            raft.add_peer(peer, ptr::null_mut());
        }
        raft.state = RaftState::Candidate(CandidateState {
            votes_responded: [0, 1, 2, 3].iter().cloned().collect(),
            votes_granted: [0, 1, 2].iter().cloned().collect(),
        });
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
        ]);

        SENT_MESSAGES.with(|messages| assert_eq!(messages.borrow().len(), 0));
        assert_eq!(raft.try_become_leader(), Ok(true));
        assert_eq!(raft.state, RaftState::Leader(LeaderState::new(
            [(0, 4), (1, 4), (2, 4), (3, 4), (4, 4)].iter().cloned().collect(),
            [(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)].iter().cloned().collect(),
        )));
        SENT_MESSAGES.with(|messages| assert_eq!(messages.borrow().len(), 4));
    }

    #[test]
    #[should_panic(expected = "try_become_leader called on non-candidate node")]
    fn test_raft_server_try_become_leader_panic_already_leader() {
        let mut raft = init_single_server();

        // `state` is `Leader`
        raft.state = RaftState::Leader(LeaderState::default());
        let _ = raft.try_become_leader();
    }

    #[test]
    #[should_panic(expected = "try_become_leader called on non-candidate node")]
    fn test_raft_server_try_become_leader_panic_state_is_follower() {
        let mut raft = init_single_server();

        // `state` is `Follower`
        raft.state = RaftState::Follower;
        let _ = raft.try_become_leader();
    }

    /* `RaftServer::client_request` tests */

    #[test]
    fn test_raft_server_client_request() {
        let mut raft = init_single_server();
        raft.current_term = 3;
        raft.state = RaftState::Leader(LeaderState::default());

        assert_eq!(raft.client_request(()), Ok(()));
        assert_eq!(raft.log, vec![LogEntry((), 3)]);
    }

    #[test]
    fn test_raft_server_client_request_not_leader() {
        let mut raft = init_single_server();

        // `state` is `Follower`
        raft.state = RaftState::Follower;
        assert_eq!(raft.client_request(()), Err(ClientRequestError::NotLeader));
    }

    /* `RaftServer::advance_commit_index` tests */

    #[test]
    fn test_raft_server_advance_commit_index() {
        let mut raft = RaftServer::new(0);
        for peer in 1..5 {
            raft.add_peer(peer, ptr::null_mut());
        }
        raft.commit_index = 1;
        raft.current_term = 3;
        raft.log.append(&mut vec![
            LogEntry((), 1),
            LogEntry((), 2),
            LogEntry((), 3),
            LogEntry((), 4),
            LogEntry((), 5),
        ]);
        raft.state = RaftState::Leader(LeaderState::new(
            HashMap::new(),
            [(1, 5), (2, 3), (3, 3), (4, 0)].iter().cloned().collect(),
        ));

        assert_eq!(raft.advance_commit_index(), Ok(()));
        assert_eq!(raft.commit_index, 3);
    }

    #[test]
    fn test_raft_server_advance_commit_index_not_matching_term() {
        let mut raft = RaftServer::new(0);
        for peer in 1..5 {
            raft.add_peer(peer, ptr::null_mut());
        }
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
        raft.state = RaftState::Leader(LeaderState::new(
            HashMap::new(),
            [(1, 5), (2, 3), (3, 3), (4, 0)].iter().cloned().collect(),
        ));

        assert_eq!(raft.advance_commit_index(), Ok(()));
        assert_eq!(raft.commit_index, 1);
    }

    #[test]
    fn test_raft_server_advance_commit_index_not_leader() {
        //let servers = [0, 1, 2, 3, 4].iter().cloned().collect();
        let mut raft = RaftServer::new(0);

        // `state` is `Follower`
        raft.state = RaftState::Follower;
        assert_eq!(raft.advance_commit_index(), Err(AdvanceCommitIndexError::NotLeader));
    }

    #[test]
    fn test_raft_server_advance_commit_index_no_quorum() {
        let mut raft = RaftServer::new(0);
        for peer in 1..5 {
            raft.add_peer(peer, ptr::null_mut());
        }
        raft.commit_index = 1;
        raft.state = RaftState::Leader(LeaderState::new(
            HashMap::new(),
            // Majority of servers don't agree on any index > 0.
            [(1, 5), (2, 3), (3, 0), (4, 0)].iter().cloned().collect(),
        ));

        assert_eq!(raft.advance_commit_index(), Ok(()));
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
            entries: vec![],
            leader_commit: 0,
        };

        let mut raft = init_single_server();

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
            entries: vec![],
            leader_commit: 0,
        };

        let mut raft = init_single_server();
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

        let mut raft = init_single_server();

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

        let mut raft = init_single_server();

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

        let mut raft = init_single_server();

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

        let mut raft = init_single_server();
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
    fn test_raft_server_handle_append_entries_response_invalid_term() {
        let mut raft = init_single_server();
        raft.state = RaftState::Leader(LeaderState::default());

        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: true,
            current_index: 0,
        };

        // `current_term` is not equal to `term`
        raft.set_current_term(0);
        raft.handle_append_entries_response(&1, &aer).unwrap();
        assert_eq!(raft.current_term, 1);
    }

    #[test]
    fn test_raft_server_handle_append_entries_response_success() {
        let mut raft = init_single_server();
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
        raft.state = RaftState::Leader(LeaderState::new(
            [(1, 7), (2, 7)].iter().cloned().collect(),
            [(1, 0), (2, 0)].iter().cloned().collect(),
        ));

        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: true,
            current_index: 0,
        };
        raft.handle_append_entries_response(&1, &aer).unwrap();
        assert_eq!(raft.state, RaftState::Leader(LeaderState::new(
            [(1, 8), (2, 7)].iter().cloned().collect(),
            [(1, 1), (2, 0)].iter().cloned().collect(),
        )));
    }

    #[test]
    fn test_raft_server_handle_append_entries_response_not_success() {
        let mut raft = init_single_server();
        raft.register_callbacks(MOCK_CALLBACKS_NOOP, ptr::null_mut());
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
        raft.state = RaftState::Leader(LeaderState::new(
            [(1, 7), (2, 7)].iter().cloned().collect(),
            [(1, 0), (2, 0)].iter().cloned().collect(),
        ));

        let aer = MessageAppendEntriesResponse {
            term: 1,
            success: false,
            current_index: 0,
        };
        raft.handle_append_entries_response(&1, &aer).unwrap();
        assert_eq!(raft.state, RaftState::Leader(LeaderState::new(
            [(1, 6), (2, 7)].iter().cloned().collect(),
            [(1, 0), (2, 0)].iter().cloned().collect(),
        )));
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

        let mut raft = init_single_server();

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

        let mut raft = init_single_server();

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

        let mut raft = init_single_server();

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

        let mut raft = init_single_server();

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
    fn test_raft_server_handle_request_vote_response_invalid_term() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: true,
        };

        let mut raft = init_single_server();

        // `current_term` is not equal to `term`
        raft.set_current_term(0);
        raft.handle_request_vote_response(&1, &rvr);
        assert_eq!(raft.current_term, 1);
    }

    #[test]
    fn test_raft_server_handle_request_vote_response_vote_granted() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: true,
        };

        let mut raft = init_single_server();
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

        let mut raft = init_single_server();
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

    /* General RPC tests */

    // Any RPC with a newer term causes the recipient to advance its term first.
    #[test]
    fn test_raft_server_rpc_update_term() {
        let rvr = MessageRequestVoteResponse {
            term: 2,
            vote_granted: false,
        };

        let mut raft = init_single_server();
        raft.state = RaftState::Candidate(CandidateState::default());
        // `current_term` is less than `term`.
        raft.current_term = 1;

        raft.handle_request_vote_response(&1, &rvr);
        assert_eq!(raft.state, RaftState::Follower);
        assert_eq!(raft.current_term, 2);
    }

    // Responses with stale terms are ignored.
    #[test]
    fn test_raft_server_receive_message_stale() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: false,
        };

        let mut raft = init_single_server();
        raft.state = RaftState::Candidate(CandidateState::default());
        // `current_term` is greater than `term`.
        raft.current_term = 2;

        raft.handle_request_vote_response(&1, &rvr);
        assert_eq!(raft.state, RaftState::Candidate(CandidateState::default()));
    }

    /* RaftServer::update_term tests */

    #[test]
    #[should_panic(expected = "assertion failed: term > self.current_term")]
    fn test_raft_server_update_term_older_term() {
        let rvr = MessageRequestVoteResponse {
            term: 1,
            vote_granted: false,
        };

        let mut raft = init_single_server();
        // `current_term` is greater than `term`.
        raft.current_term = 2;

        raft.update_term(&1, rvr.term);
    }

    #[test]
    fn test_raft_server_update_term_newer_term() {
        let rvr = MessageRequestVoteResponse {
            term: 2,
            vote_granted: false,
        };

        let mut raft = init_single_server();
        // `current_term` is less than `term`.
        raft.current_term = 1;
        raft.state = RaftState::Leader(LeaderState::default());
        raft.voted_for = Some(1);

        raft.update_term(&1, rvr.term);
        assert_eq!(raft.current_term, 2);
        assert_eq!(raft.state, RaftState::Follower);
        assert_eq!(raft.voted_for, None);
    }

    /* `RaftServer::periodic` tests */

    #[test]
    fn test_raft_server_periodic_follower_election_timeout_elapses() {
        let mut raft = init_single_server();
        raft.register_callbacks(MOCK_CALLBACKS_NOOP, ptr::null_mut());

        raft.election_timer = 0;
        raft.election_timeout = 150;
        assert!(raft.is_follower());
        raft.periodic(200).unwrap();
        assert!(raft.is_candidate());
    }

    #[test]
    fn test_raft_server_periodic_follower_election_timeout_elapses_vote_already_granted() {
        let servers: HashSet<Id> = [0, 1].iter().cloned().collect();
        let mut raft = RaftServer::new(0);
        raft.register_callbacks(MOCK_CALLBACKS_NOOP, ptr::null_mut());

        raft.set_voted_for(Some(1));
        raft.election_timer = 0;
        raft.election_timeout = 150;
        assert!(raft.is_follower());
        raft.periodic(200).unwrap();
        assert!(raft.is_follower());
    }

    /* `RaftServer::start_election` tests */

    #[test]
    fn test_raft_server_start_election() {
        let mut raft = RaftServer::new(0);
        raft.register_callbacks(MOCK_CALLBACKS, ptr::null_mut());
        for peer in 1..4 {
            raft.add_peer(peer, ptr::null_mut());
        }

        raft.state = RaftState::Candidate(CandidateState::default());
        raft.election_timer = 10;
        raft.set_current_term(5);
        raft.start_election().unwrap();
        assert_eq!(raft.current_term, 6);
        assert_eq!(raft.state, RaftState::Candidate(CandidateState {
            votes_responded: [0].iter().cloned().collect(),
            votes_granted: [0].iter().cloned().collect(),
        }));
        assert_eq!(raft.voted_for, Some(raft.node.id));
        assert_eq!(raft.election_timer, 0);
        SENT_MESSAGES.with(|messages| {
            assert_eq!(messages.borrow().len(), 3);
        });
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.is_candidate()")]
    fn test_raft_server_start_election_is_not_candidate() {
        let mut raft = init_single_server();

        raft.start_election().unwrap();
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

    #[test]
    fn test_is_election_timeout_elapsed() {
        let mut raft = init_single_server();

        raft.election_timer = 100;
        raft.election_timeout = 150;
        assert!(!raft.is_election_timeout_elapsed());
        raft.election_timer = 200;
        assert!(raft.is_election_timeout_elapsed());
    }
}
