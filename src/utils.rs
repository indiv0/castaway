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
