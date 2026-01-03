FROM rust:1.92-bookworm as builder

WORKDIR /usr/src/app
COPY . .

WORKDIR /usr/src/app/curio-processor
RUN cargo build --release

FROM debian:bookworm-slim

COPY --from=builder /usr/src/app/curio-processor/target/release/curio-processor /usr/local/bin/curio-processor
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

CMD ["curio-processor"]
