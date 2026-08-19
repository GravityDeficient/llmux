FROM rust:1.88-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 llmux \
    && useradd --system --uid 65532 --gid llmux --home-dir /var/lib/llmux --create-home llmux

COPY --from=builder /src/target/release/llmux /usr/local/bin/llmux

USER llmux:llmux
WORKDIR /var/lib/llmux
EXPOSE 18080

ENTRYPOINT ["/usr/local/bin/llmux"]
CMD ["--config", "/etc/llmux/config.yaml"]
