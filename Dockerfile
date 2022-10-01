FROM rust
WORKDIR /app
COPY ./Cargo.toml /app
COPY ./Cargo.lock /app
COPY ./src /app/src
RUN cargo build --release
RUN strip target/release/rosm
COPY ./import.sh /app/import.sh

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y curl
COPY --from=0 /app/target/release/rosm .
COPY --from=0 /app/import.sh .
ENTRYPOINT ["./import.sh"]
