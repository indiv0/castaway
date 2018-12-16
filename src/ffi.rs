// TODO: ensure all pointers are not null.
use errors::*;
use libc::{c_char, c_int, size_t};
use raft::{
    Callbacks,
    Command,
    Id,
    LogEntry,
    MessageAppendEntriesRaw,
    MessageAppendEntriesResponse,
    MessageRequestVote,
    MessageRequestVoteResponse,
    NodeInfo,
    RaftServer,
    Term,
    UserData,
};
use std::cell::RefCell;
use std::error::Error as StdError;
use std::slice;
use std::mem::transmute;
use std::ptr;

// TODO: make the error codes negative.
// SEE: https://github.com/eqrion/cbindgen/issues/205
pub const CASTAWAY_ENULLPOINTER: i32 = 2;
pub const CASTAWAY_EBUFTOOSMALL: i32 = 3;
pub const CASTAWAY_OPT_SOME: isize = 1;
pub const CASTAWAY_OPT_NONE: isize = 0;

// FROM: https://michael-f-bryan.github.io/rust-ffi-guide/errors/return_types.html
thread_local!{
    static LAST_ERROR: RefCell<Option<Box<StdError>>> = RefCell::new(None);
}


/// Update the most recent error, clearing whatever may have been there before.
pub fn update_last_error<E: StdError + 'static>(err: E) {
    error!("Setting LAST_ERROR: {}", err);

    {
        // Print a pseudo-backtrace for this error, following back each error's
        // cause until we reach the root error.
        let mut cause = err.cause();
        while let Some(parent_err) = cause {
            warn!("Caused by: {}", parent_err);
            cause = parent_err.cause();
        }
    }

    LAST_ERROR.with(|prev| {
        *prev.borrow_mut() = Some(Box::new(err));
    });
}

/// Retrieve the most recent error, clearing it in the process.
pub fn take_last_error() -> Option<Box<StdError>> {
    LAST_ERROR.with(|prev| prev.borrow_mut().take())
}

/// Calculate the number of bytes in the last error's error message **not**
/// including any trailing `null` characters.
#[no_mangle]
pub extern "C" fn last_error_length() -> c_int {
    LAST_ERROR.with(|prev| match *prev.borrow() {
        Some(ref err) => err.to_string().len() as c_int + 1,
        None => 0,
    })
}

/// Write the most recent error message into a caller-provided buffer as a UTF-8
/// string, returning the number of bytes written.
///
/// # Note
///
/// This writes a **UTF-8** string into the buffer. Windows users may need to
/// convert it to a UTF-16 "unicode" afterwards.
///
/// If there are no recent errors then this returns `0` (because we wrote 0
/// bytes). `-1` is returned if there are any errors, for example when passed a
/// null pointer or a buffer of insufficient size.
#[no_mangle]
pub unsafe extern "C" fn last_error_message(buffer: *mut c_char, length: c_int) -> c_int {
    if buffer.is_null() {
        warn!("Null pointer passed into last_error_message() as the buffer");
        return CASTAWAY_ENULLPOINTER;
    }

    let last_error = match take_last_error() {
        Some(err) => err,
        None => return 0,
    };

    let error_message = last_error.to_string();

    let buffer = slice::from_raw_parts_mut(buffer as *mut u8, length as usize);

    if error_message.len() >= buffer.len() {
        warn!("Buffer provided for writing the last error message is too small.");
        warn!(
            "Expected at least {} bytes but got {}",
            error_message.len() + 1,
            buffer.len()
        );
        return CASTAWAY_EBUFTOOSMALL;
    }

    ptr::copy_nonoverlapping(
        error_message.as_ptr(),
        buffer.as_mut_ptr(),
        error_message.len(),
    );

    // Add a trailing null so people using the string as a `char *` don't
    // accidentally read into garbage.
    buffer[error_message.len()] = 0;

    error_message.len() as c_int
}

/// Instantiate a `RaftServer` in memory and return a pointer to it.
#[no_mangle]
pub extern "C" fn raft_server_new(id: Id) -> *mut RaftServer {
    // FIXME: remove this
    /*
    let servers = CVec::new(servers_ptr, servers_len);
    let servers = servers.iter().cloned().collect();
    */
    let raft = RaftServer::new(id);
    unsafe { transmute(Box::new(raft)) }
}

/// Destroy a `RaftServer` object.
#[no_mangle]
pub extern "C" fn raft_server_free(ptr: *mut RaftServer) {
    let _raft: Box<RaftServer> = unsafe { transmute(ptr) };
    // Drop
}

#[no_mangle]
pub extern "C" fn raft_server_register_callbacks(raft_ptr: *mut RaftServer, callbacks_ptr: *mut Callbacks, user_data_ptr: UserData) {
    let _raft = unsafe { &mut *raft_ptr };
    let _callbacks = unsafe { &mut *callbacks_ptr };
    _raft.register_callbacks(_callbacks.clone(), user_data_ptr);
}

#[no_mangle]
pub extern "C" fn raft_server_add_peer(raft_ptr: *mut RaftServer, peer_id: Id, user_data_ptr: UserData) {
    let _raft = unsafe { &mut *raft_ptr };
    _raft.add_peer(peer_id, user_data_ptr);
}

#[no_mangle]
pub extern "C" fn raft_server_get_node_info_by_id(raft_ptr: *mut RaftServer, id: Id, node_info: *mut *const NodeInfo) -> c_int {
    let _raft = unsafe { &mut *raft_ptr };
    let node = _raft.get_node_info_by_id(id);
    if let Some(node) = node {
        unsafe { *node_info = node };
    } else {
        unsafe { *node_info = ptr::null() };
        return CASTAWAY_OPT_NONE as c_int;
    }

    CASTAWAY_OPT_SOME as c_int
}

#[no_mangle]
pub extern "C" fn raft_server_recv_append_entries(raft_ptr: *mut RaftServer, peer: &Id, _msg: MessageAppendEntriesRaw) {
    let _raft = unsafe { &mut *raft_ptr };
    let msg = _msg.into();
    _raft.handle_append_entries_request(peer, &msg);
}

#[no_mangle]
pub extern "C" fn raft_server_periodic(ptr: *mut RaftServer, ms_since_last_period: usize) {
    let mut _raft = unsafe { &mut *ptr };
    _raft.periodic(ms_since_last_period).unwrap();
}

#[no_mangle]
pub extern "C" fn raft_server_voted_for(ptr: *mut RaftServer, peer: *mut Id) -> c_int {
    // FIXME
    if ptr.is_null() {
        let err = Error::from("Null pointer passed into raft_server_voted_for() as the server");
        update_last_error(err);
        return CASTAWAY_ENULLPOINTER as c_int;
    }
    let _raft = unsafe { &mut *ptr };
    if peer.is_null() {
        let err = Error::from("Null pointer passed into raft_server_voted_for() as the peer");
        update_last_error(err);
        return CASTAWAY_ENULLPOINTER;
    }
    let _peer = unsafe { &mut *peer };

    if let Some(v) = _raft.voted_for() {
        *_peer = v;
    } else {
        return CASTAWAY_OPT_NONE as c_int;
    }

    return CASTAWAY_OPT_SOME as c_int;
}

#[no_mangle]
pub extern "C" fn raft_server_handle_request_vote_request(ptr: *mut RaftServer, peer: Id, msg: *const MessageRequestVote) -> MessageRequestVoteResponse {
    let mut _raft = unsafe { &mut *ptr };
    let _msg = unsafe { &*msg };
    _raft.handle_request_vote_request(&peer, _msg)
}

#[no_mangle]
pub extern "C" fn raft_server_handle_request_vote_response(ptr: *mut RaftServer, peer: Id, msg: *const MessageRequestVoteResponse) {
    let mut _raft = unsafe { &mut *ptr };
    let _msg = unsafe { &*msg };
    _raft.handle_request_vote_response(&peer, _msg);
}

#[no_mangle]
pub extern "C" fn raft_server_handle_append_entries_request(ptr: *mut RaftServer, peer: Id, msg: *const MessageAppendEntriesRaw) -> MessageAppendEntriesResponse {
    let mut _raft = unsafe { &mut *ptr };
    let _msg = unsafe { &*msg };
    _raft.handle_append_entries_request(&peer, &(*_msg).clone().into())
}

#[no_mangle]
pub extern "C" fn raft_server_handle_append_entries_response(ptr: *mut RaftServer, peer: Id, msg: *const MessageAppendEntriesResponse) {
    let mut _raft = unsafe { &mut *ptr };
    let _msg = unsafe { &*msg };
    _raft.handle_append_entries_response(&peer, _msg).unwrap();
}

#[no_mangle]
pub extern "C" fn raft_server_client_request(ptr: *mut RaftServer, command: Command, index: *mut *const usize, term: *mut *const Term) {
    let mut _raft = unsafe { &mut *ptr };
    let (entry_index, entry_term) = _raft.client_request(command).unwrap();
    unsafe {
        *index = &entry_index as *const usize;
        *term = &entry_term as *const Term;
    };
}

#[no_mangle]
pub extern "C" fn raft_server_current_index(ptr: *mut RaftServer) -> usize {
    let _raft = unsafe { &*ptr };
    _raft.current_index()
}

#[no_mangle]
pub extern "C" fn raft_server_is_leader(ptr: *mut RaftServer) -> bool {
    let mut _raft = unsafe { &*ptr };
    _raft.is_leader()
}

#[no_mangle]
pub extern "C" fn raft_server_get_log(ptr: *mut RaftServer, buffer: *mut *const LogEntry, length: *mut size_t) {
    let mut _raft = unsafe { &*ptr };

    //let buffer = unsafe { slice::from_raw_parts_mut(buffer, length) };
    unsafe {
        *buffer = _raft.log.inner.as_ptr();
        *length = _raft.log.inner.len() as size_t;
        //ptr::copy_nonoverlapping(_raft.log.inner.as_ptr(), buffer.as_mut_ptr(), length);
    }
}
