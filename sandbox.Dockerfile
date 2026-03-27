FROM oven/bun:alpine

RUN apk add --no-cache git ca-certificates

WORKDIR /workspace
