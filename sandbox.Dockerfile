FROM debian:stable-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git ca-certificates \
    && curl -fsSL https://bun.sh/install | bash \
    && ln -s /root/.bun/bin/bun /usr/local/bin/bun \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
