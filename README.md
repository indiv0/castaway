# castaway

## Development

Build the docker container:

    docker build . -t castaway:latest

Run a single build:

    docker run --rm \
        -v "$PWD":/usr/src/castaway \
        -w /usr/src/castaway \
        castaway:latest \
        cargo build --release

Open a development container:

    docker run --rm -it \
        -v "$PWD":/usr/src/castaway \
        -w /usr/src/castaway \
        castaway:latest \
        /bin/bash

Run an instance of castaway:

    cargo run -- --tls

Run the docker container:

    docker run --rm castaway:latest

To launch a cluster of containers locally:

    docker-compose up --build

C bindings can be acquired by compiling the library with the `cbindgen` feature
enabled:

    cargo build --features cbindgen

The bindings will be placed in `target/$PROFILE/` (e.g. `target/debug/castaway.h`).

## Credits

* Raft interface inspired by [willemt/raft][willemt-raft]

[willemt-raft]: https://github.com/willemt/raft
