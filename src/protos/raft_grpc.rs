// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]


// interface

pub trait Raft {
    fn send_ping(&self, o: ::grpc::RequestOptions, p: super::raft::PingRequest) -> ::grpc::SingleResponse<super::raft::PingReply>;
}

// client

pub struct RaftClient {
    grpc_client: ::grpc::Client,
    method_SendPing: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::raft::PingRequest, super::raft::PingReply>>,
}

impl RaftClient {
    pub fn with_client(grpc_client: ::grpc::Client) -> Self {
        RaftClient {
            grpc_client: grpc_client,
            method_SendPing: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/castaway.Raft/SendPing".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
        }
    }

    pub fn new_plain(host: &str, port: u16, conf: ::grpc::ClientConf) -> ::grpc::Result<Self> {
        ::grpc::Client::new_plain(host, port, conf).map(|c| {
            RaftClient::with_client(c)
        })
    }
    pub fn new_tls<C : ::tls_api::TlsConnector>(host: &str, port: u16, conf: ::grpc::ClientConf) -> ::grpc::Result<Self> {
        ::grpc::Client::new_tls::<C>(host, port, conf).map(|c| {
            RaftClient::with_client(c)
        })
    }
}

impl Raft for RaftClient {
    fn send_ping(&self, o: ::grpc::RequestOptions, p: super::raft::PingRequest) -> ::grpc::SingleResponse<super::raft::PingReply> {
        self.grpc_client.call_unary(o, p, self.method_SendPing.clone())
    }
}

// server

pub struct RaftServer;


impl RaftServer {
    pub fn new_service_def<H : Raft + 'static + Sync + Send + 'static>(handler: H) -> ::grpc::rt::ServerServiceDefinition {
        let handler_arc = ::std::sync::Arc::new(handler);
        ::grpc::rt::ServerServiceDefinition::new("/castaway.Raft",
            vec![
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/castaway.Raft/SendPing".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.send_ping(o, p))
                    },
                ),
            ],
        )
    }
}
