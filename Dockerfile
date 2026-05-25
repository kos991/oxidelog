FROM rust:1-bookworm AS builder

WORKDIR /src
COPY . .
RUN cargo build --release -p fwlogd

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/fwlogd /usr/local/bin/fwlogd

EXPOSE 18080 1514 1515/udp
ENTRYPOINT ["fwlogd"]
