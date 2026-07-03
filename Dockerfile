FROM rust:1.96-bookworm AS builder

WORKDIR /usr/src/app

COPY . .

ENV SQLX_OFFLINE=true
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/smart-http-requester-worker-rust /usr/local/bin/smart-http-requester-worker-rust
COPY --from=builder /usr/src/app/Settings.yml /Settings.yml

CMD ["smart-http-requester-worker-rust"]