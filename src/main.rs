extern crate clap;
extern crate futures;
extern crate grpc;
extern crate protobuf;
extern crate tls_api;
extern crate tls_api_native_tls;

use clap::{Arg, App};
use std::thread;
use protos::raft::{PingRequest, PingReply};
use protos::raft_grpc::{Raft, RaftServer};
use tls_api::TlsAcceptorBuilder;

mod protos;

const PORT: u16 = 50051;
const PORT_TLS: u16 = 50052;

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

fn main() {
    // clap-rs matches, used for argument parsing.
    let matches = App::new("castaway")
        .version("0.1")
        .author("Nikita Pekin <npeki029@uottawa.ca>")
        .about("Raft Server")
        .arg(Arg::with_name("tls")
             .long("tls")
             .help("Initialize server with TLS enabled"))
        .arg(Arg::with_name("port")
             .short("p")
             .long("port")
             .value_name("LISTEN_PORT")
             .help("Port to listen on")
             .takes_value(true))
        .get_matches();

    let tls = matches.is_present("tls");
    let port = match matches.value_of("port") {
        Some(port) => port.parse().unwrap(),
        None => if tls { PORT_TLS } else { PORT },
    };

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
