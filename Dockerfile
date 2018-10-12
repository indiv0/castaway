FROM rust:1.29.1-stretch

RUN apt-get update
RUN apt-get install -y protobuf-compiler

WORKDIR /usr/src/castaway
COPY . .

RUN cargo build

CMD ["target/debug/castaway"]
