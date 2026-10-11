# syntax=docker/dockerfile:1.7

# ---- build ------------------------------------------------------------------
FROM golang:1.24-alpine AS build
WORKDIR /src
ARG VERSION=dev
ARG COMMIT=unknown
COPY go.mod ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/UDogg/DistKV/internal/version.Version=${VERSION} -X github.com/UDogg/DistKV/internal/version.Commit=${COMMIT}" \
      -o /out/ ./cmd/... \
 && mkdir -p /out/data

# ---- runtime ----------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.title="DistKV" \
      org.opencontainers.image.description="Raft-based sharded distributed key-value cache" \
      org.opencontainers.image.source="https://github.com/UDogg/DistKV" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/distkv /out/distkvctl /out/distkv-bench /out/distkv-demo /usr/local/bin/
# /data must belong to the runtime user: a fresh named volume copies this
# ownership, otherwise the non-root server could not write its WAL.
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot:nonroot
WORKDIR /data
VOLUME ["/data"]
# 7001: node API + dashboard (/ui/). 8080: distkv-demo dashboard.
EXPOSE 7001 8080
ENV DISTKV_DATA_DIR=/data
ENTRYPOINT ["/usr/local/bin/distkv"]
