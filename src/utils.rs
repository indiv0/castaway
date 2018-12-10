/*
//! Extra utility functions.

use chrono::Local;
use errors::*;
use fern;
use log::LevelFilter;
use std::sync::{Once, ONCE_INIT};

/// Initialize the global logger and log to `castaway.log`.
///
/// Note that this is an idempotent function, so you can call it as many times
/// as you want and logging will only be initialized the first time.
// FROM: https://github.com/Michael-F-Bryan/rust-ffi-guide/blob/80e56e297a8f17d3a722ac83bab6701ef1850567/client/src/utils.rs
#[no_mangle]
pub extern "C" fn initialize_logging() {
    static INITIALIZE: Once = ONCE_INIT;
    INITIALIZE.call_once(|| {
        fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!(
                    "{} {:7} ({}#{}): {}{}",
                    Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                    record.level(),
                    record.module_path().unwrap(),
                    record.line().unwrap(),
                    message,
                    if cfg!(windows) { "\r" } else { "" }
                ))
            })
            .level(LevelFilter::Debug)
            .chain(fern::log_file("castaway.log").unwrap())
            .apply()
            .unwrap();
    });
}

/// Log an error and each successive error which caused it.
// FROM: https://github.com/Michael-F-Bryan/rust-ffi-guide/blob/80e56e297a8f17d3a722ac83bab6701ef1850567/client/src/utils.rs
pub fn backtrace(e: &Error) {
    error!("Error: {}", e);

    for cause in e.iter().skip(1) {
        warn!("\tCaused by: {}", cause);
    }
}
*/
use libc::{c_void, size_t};
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::slice;

pub struct CVec<T> {
    ptr: *const T,
    len: usize,
}

impl<T> CVec<T> {
    pub fn new(ptr: *const T, len: usize) -> Self {
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

// FIXME: get rid of this
/*
#[repr(C)]
pub struct Tuple {
    a: uint32_t,
    b: uint32_t,
}
*/

#[derive(Debug, PartialEq)]
#[repr(C)]
pub struct Array<T> {
    data: *mut c_void,
    len: size_t,
    phantom: PhantomData<T>,
}

impl<T> Array<T> {
    #[allow(dead_code)]
    unsafe fn as_u32_slice(&self) -> &[u32] {
        assert!(!self.data.is_null());
        slice::from_raw_parts(self.data as *const u32, self.len as usize)
    }

    pub fn from_vec(mut vec: Vec<T>) -> Self {
        // Important to make length and capacity match.
        // A better solution is to track both length and capacity.
        vec.shrink_to_fit();

        let array = Array { data: vec.as_mut_ptr() as *mut c_void, len: vec.len() as size_t, phantom: PhantomData };

        // Leak the memory, and now the raw pointer (and eventually C) is the
        // owner.
        mem::forget(vec);

        array
    }

    pub fn to_vec(self) -> Vec<T> {
        let len = self.len as usize;
        let vec = unsafe { Vec::from_raw_parts(self.data as *mut T, len, len) };
        vec
    }
}

impl<T> Clone for Array<T>
    where T: Clone,
{
    fn clone(&self) -> Self {
        let len = self.len as usize;
        let vec = unsafe { Vec::from_raw_parts(self.data as *mut T, len, len) };
        let new_vec = vec.clone();
        mem::forget(vec);
        Array::from_vec(new_vec)
    }
}

/*
#[no_mangle]
pub extern "C" fn convert_vec(a: Array, b: Array) -> Array {
    let a = unsafe { a.as_u32_slice() };
    let b = unsafe { b.as_u32_slice() };

    let vec =
        a.iter().zip(b.iter())
        .map(|(&a, &b)| Tuple { a, b })
        .collect();

    Array::from_vec(vec)
}
*/

