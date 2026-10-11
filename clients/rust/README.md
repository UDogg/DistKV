# distkv (Rust) 🦀

A dependency-free (standard library only) client for DistKV, plus **`distkv-top`**, an htop-style
live terminal view of the cluster.

```rust
let c = distkv::Client::connect(&["http://127.0.0.1:7101"])?;
let v = c.put("user:42", b"alice", Some(std::time::Duration::from_secs(60)))?;
let item = c.get("user:42")?.expect("present");
match c.compare_and_swap("user:42", v, b"bob") {
    Err(distkv::Error::Conflict { current_version }) => println!("now at {current_version}"),
    other => { other?; }
}
```

```bash
cargo run --release --bin distkv-top -- --endpoints http://127.0.0.1:7101,http://127.0.0.1:7102
cargo run --release --bin distkv-top -- --once      # one frame, for scripts and CI
```

What's inside: a strict RFC 8259 JSON parser (`json.rs`), an HTTP/1.1 client with chunked
decoding (`http.rs`), routing verified against the Go golden vectors (`routing.rs`), and a smart
client that follows leader hints (`lib.rs`). It supports plain `http://` only, so put a
TLS-terminating proxy in front of mTLS clusters.

```bash
cargo test                                            # unit + golden vectors
DISTKV_ENDPOINTS=http://127.0.0.1:7101 cargo test -- --include-ignored   # + live cluster
```
