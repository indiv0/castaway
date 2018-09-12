extern crate grpc;
extern crate protobuf;
extern crate tls_api;

use protos::raft::{PingRequest, PingReply};
use protos::raft_grpc::Raft;

mod protos;

struct RaftImpl;

impl Raft for RaftImpl {
    fn send_ping(&self, _m: grpc::RequestOptions, req: PingRequest) -> grpc::SingleResponse<PingReply> {
        println!("ping received");
        let mut r = PingReply::new();
        let name = if req.get_message().is_empty() { "pong" } else { req.get_message() };
        r.set_message(name.to_string());
        grpc::SingleResponse::completed(r)
    }
}

fn main() {
    println!("Hello, world!");
}
