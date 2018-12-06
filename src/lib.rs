// Set high recursion limit as `error_chain!` can recurse deeply.
#![recursion_limit = "1024"]

extern crate chrono;
#[macro_use]
extern crate error_chain;
extern crate fern;
extern crate libc;
#[macro_use]
extern crate log;
extern crate rand;

mod call;
mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {}
}
mod ffi;
mod raft;
mod utils;

// TODO: get rid of these globs
pub use ffi::*;
pub use raft::*;
pub use utils::*;
