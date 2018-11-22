use raft::{Id, RaftServer};
use std::slice;
use std::mem::transmute;
use std::ops::Deref;

struct CVec<T> {
    ptr: *const T,
    len: usize,
}

impl<T> CVec<T> {
    fn new(ptr: *const T, len: usize) -> Self {
        assert!(!ptr.is_null());
        Self {
            ptr,
            len,
        }
    }
}

impl<T> Deref for CVec<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Instantiate a `RaftServer` in memory and return a pointer to it.
#[no_mangle]
pub extern "C" fn raft_server_new(id: Id, servers_ptr: *const usize, servers_len: usize) -> *mut RaftServer {
    let servers = CVec::new(servers_ptr, servers_len);
    let servers = servers.iter().cloned().collect();

    let raft = RaftServer::new(id, servers);
    unsafe { transmute(Box::new(raft)) }
}

/// Destroy a `RaftServer` object.
#[no_mangle]
pub extern "C" fn raft_server_free(ptr: *mut RaftServer) {
    let _raft: Box<RaftServer> = unsafe { transmute(ptr) };
    // Drop
}
