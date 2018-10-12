FROM rust:1.29.1-stretch

RUN apt-get update && \
    apt-get install -y \
        protobuf-compiler \
        libzmq3-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    groupadd castaway && useradd castaway -ms /bin/bash -g castaway

RUN mkdir -p /usr/src/castaway && \
    chown castaway:castaway /usr/src/castaway
WORKDIR /usr/src/castaway
COPY --chown=castaway:castaway . .

USER castaway
RUN cargo build

CMD ["target/debug/castaway"]
