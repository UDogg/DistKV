# Contributing to DistKV

## Ground rules

* **Zero dependencies.** DistKV uses only the Go standard library. Please don't add modules to
  `go.mod`; tools run via `go run pkg@version` in CI are fine.
* **Correctness first.** Any change to `internal/raft` or `internal/kv` must keep every test in
  `internal/raft`, `internal/kv` and `test/e2e` passing under `-race`. A change that affects safety
  needs a test that fails without it, and an update to the argument in `docs/RAFT.md`.
* **Measure performance claims.** Use `make bench` / `scripts/chaos-bench.sh` before and after,
  and include the numbers in the PR.

## Workflow

```bash
make fmt          # gofmt -s
make lint         # gofmt check + go vet (CI also runs staticcheck)
make test-short   # fast loop (~40s)
make race         # full suite incl. chaos tests under the race detector
make fuzz         # fuzz the Raft codec
```

Raft debugging: `RAFT_DEBUG=1 go test -run TestName -v ./internal/raft/` turns on debug logs for
the in-memory harness. `DISTKV_TEST_DEBUG=1` does the same for in-process clusters.

## Code layout conventions

* Consensus code never performs network or disk I/O while holding `Node.mu`, except for the
  follower's append-path storage writes and hard-state writes, which Raft requires before replying.
* State machine `Apply` must be deterministic: no wall clock, randomness or map-iteration order.
  Snapshots must use canonical ordering. `TestDeterminismAndSnapshot` enforces this.
* Unrecoverable storage errors panic (`Node.fatal`); never continue after a failed durable write.
* Wire formats (`internal/transport/codec.go`, `internal/kv/command.go`, WAL records) carry a
  version byte. Bump it, and keep decoding the old version, when you change a format.

## Commit messages

Use the imperative mood ("Add lease reads", not "Added lease reads"). Explain *why* in the body,
with measurements for performance work.
