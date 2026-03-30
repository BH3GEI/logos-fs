FROM rust:1.94-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev cmake protobuf-compiler libprotobuf-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build
RUN cargo build -p logos-session

# Run in-memory tests (no Ollama needed)
CMD ["cargo", "test", "-p", "logos-session"]
