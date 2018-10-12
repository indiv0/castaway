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

Run the docker container:

    docker run --rm castaway:latest
