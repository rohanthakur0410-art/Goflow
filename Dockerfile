# --- build stage -------------------------------------------------------
FROM golang:1.22-bookworm AS build

WORKDIR /src

# go-sqlite3 uses cgo, so the build stage needs a C toolchain. The
# golang:*-bookworm image already ships gcc/libc headers.
ENV CGO_ENABLED=1

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/goflow-server ./cmd/server

# --- runtime stage -------------------------------------------------------
FROM debian:bookworm-slim

RUN useradd --system --create-home --home-dir /app --shell /usr/sbin/nologin goflow \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /out/goflow-server ./goflow-server
COPY migrations ./migrations
COPY web ./web

RUN mkdir -p /app/data && chown -R goflow:goflow /app

USER goflow

ENV PORT=8080 \
    DATABASE_PATH=/app/data/goflow.db

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f "http://localhost:${PORT}/api/health" || exit 1

ENTRYPOINT ["/app/goflow-server"]
