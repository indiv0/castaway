extern crate futures;
extern crate grpc;
extern crate protobuf;
extern crate tls_api;
extern crate tls_api_native_tls;

use std::env;
use std::thread;
use protos::raft::{PingRequest, PingReply};
use protos::raft_grpc::{Raft, RaftServer};
use tls_api::TlsAcceptorBuilder;

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

fn test_tls_acceptor() -> tls_api_native_tls::TlsAcceptor {
    let pkcs12 = include_bytes!("../foobar.com.p12");
    let builder = tls_api_native_tls::TlsAcceptorBuilder::from_pkcs12(pkcs12, "mypass").unwrap();
    builder.build().unwrap()
}

fn is_tls() -> bool {
    env::args().any(|a| a == "--tls")
}

fn main() {
    let tls = is_tls();

    let port = if tls { 50052 } else { 50051 };

    let mut server = grpc::ServerBuilder::new();
    server.http.set_port(port);
    server.add_service(RaftServer::new_service_def(RaftImpl));
    server.http.set_cpu_pool_threads(4);
    if tls {
        server.http.set_tls(test_tls_acceptor());
    }
    let _server = server.build().expect("server");

    println!("raft server started on port {} {}",
        port, if tls { "with tls" } else { "without tls" });

    loop {
        thread::park();
    }
}
