FROM rust:1.94.1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src ./src
COPY config ./config
COPY migrations ./migrations
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/liquidation-engine /usr/local/bin/liquidation-engine
COPY --from=builder /app/target/release/replay /usr/local/bin/replay
COPY --from=builder /app/config ./config
COPY fixtures ./fixtures
USER 10001:10001
ENTRYPOINT ["liquidation-engine", "--config", "config/arbitrum.toml", "--observe-only"]

