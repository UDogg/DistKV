## What & why

## How was this verified?
- [ ] `make race` (Go, incl. chaos tests)
- [ ] `make polyglot` (Python, TypeScript, Rust SDKs + dashboard type-check)
- [ ] Routing unchanged, or vectors regenerated deliberately (`go test ./internal/cluster -run TestRoutingVectors -update`)
- [ ] Benchmarks before/after (for performance changes)

## Safety notes
<!-- Does this touch Raft, storage or the state machine? Explain why it stays correct. -->
