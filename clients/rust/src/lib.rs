//! # DistKV for Rust
//!
//! A dependency-free client for [DistKV](https://github.com/UDogg/DistKV)
//! plus the `distkv-top` terminal monitor. Routing is bit-for-bit identical
//! to the Go servers, so requests go straight to shard leaders.
//!
//! ```no_run
//! let c = distkv::Client::connect(&["http://127.0.0.1:7001"]).unwrap();
//! c.put("user:42", b"alice", None).unwrap();
//! assert_eq!(c.get("user:42").unwrap().unwrap().value, b"alice");
//! ```

pub mod http;
pub mod json;
pub mod routing;

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub use routing::{hash64, Member, Topology};

#[derive(Debug, Clone)]
pub struct Item {
    pub key: String,
    pub value: Vec<u8>,
    pub version: u64,
    pub shard: usize,
    pub served_by: String,
}

#[derive(Debug)]
pub enum Error {
    Conflict { current_version: u64 },
    Unavailable(String),
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Conflict { current_version } => write!(f, "version conflict (current version {current_version})"),
            Error::Unavailable(m) => write!(f, "unavailable: {m}"),
            Error::Other(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {}

/// Fetch and parse `GET /v1/cluster` from the first reachable endpoint.
pub fn cluster_status(endpoints: &[&str], timeout: Duration) -> Result<json::Value, Error> {
    let mut last = String::from("no endpoints");
    for ep in endpoints {
        match http::request("GET", &format!("{}/v1/cluster", ep.trim_end_matches('/')), &[], None, timeout) {
            Ok(r) if r.status == 200 => return json::parse(&r.text()).map_err(|e| Error::Other(e.to_string())),
            Ok(r) => last = format!("{ep}: HTTP {}", r.status),
            Err(e) => last = format!("{ep}: {e}"),
        }
    }
    Err(Error::Unavailable(last))
}

/// A smart client (single-threaded; create one per thread).
pub struct Client {
    pub topology: Topology,
    leaders: RefCell<HashMap<usize, String>>,
    timeout: Duration,
}

impl Client {
    pub fn connect(endpoints: &[&str]) -> Result<Client, Error> {
        let timeout = Duration::from_secs(5);
        let st = cluster_status(endpoints, timeout)?;
        let spec = st.get("spec");
        let members = spec
            .get("members")
            .as_array()
            .iter()
            .map(|m| Member { id: m.get("id").as_str().into(), addr: m.get("addr").as_str().into() })
            .collect();
        let topology = Topology::new(
            members,
            spec.get("shards").as_u64() as usize,
            spec.get("replication_factor").as_u64() as usize,
            spec.get("virtual_nodes").as_u64() as usize,
        )
        .map_err(Error::Other)?;
        let leaders = st
            .get("shards")
            .as_array()
            .iter()
            .filter(|s| !s.get("leader_addr").as_str().is_empty())
            .map(|s| (s.get("shard").as_u64() as usize, s.get("leader_addr").as_str().to_string()))
            .collect();
        Ok(Client { topology, leaders: RefCell::new(leaders), timeout })
    }

    pub fn get(&self, key: &str) -> Result<Option<Item>, Error> {
        let r = self.call("GET", key, "", None)?;
        match r.status {
            200 => Ok(Some(Item {
                key: key.into(),
                value: r.body.clone(),
                version: r.header("X-Distkv-Version").and_then(|v| v.parse().ok()).unwrap_or(0),
                shard: r.header("X-Distkv-Shard").and_then(|v| v.parse().ok()).unwrap_or(0),
                served_by: r.header("X-Distkv-Served-By").unwrap_or("").into(),
            })),
            404 => Ok(None),
            s => Err(Error::Other(format!("HTTP {s}: {}", r.text()))),
        }
    }

    /// Write a key; `ttl` expires it. Returns the new version.
    pub fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<u64, Error> {
        let q = ttl.map(|t| format!("?ttl={}ms", t.as_millis())).unwrap_or_default();
        self.write(key, &q, value)
    }

    /// Compare-and-set: write only if the current version is `expected` (0 = create).
    pub fn compare_and_swap(&self, key: &str, expected: u64, value: &[u8]) -> Result<u64, Error> {
        self.write(key, &format!("?if_version={expected}"), value)
    }

    pub fn delete(&self, key: &str) -> Result<bool, Error> {
        let r = self.call("DELETE", key, "", None)?;
        match r.status {
            200 => Ok(true),
            404 => Ok(false),
            s => Err(Error::Other(format!("HTTP {s}: {}", r.text()))),
        }
    }

    fn write(&self, key: &str, query: &str, value: &[u8]) -> Result<u64, Error> {
        let r = self.call("PUT", key, query, Some(value))?;
        let doc = json::parse(&r.text()).unwrap_or(json::Value::Null);
        match r.status {
            200 => Ok(doc.get("version").as_u64()),
            409 => Err(Error::Conflict { current_version: doc.get("current_version").as_u64() }),
            s => Err(Error::Other(format!("HTTP {s}: {}", r.text()))),
        }
    }

    fn call(&self, method: &str, key: &str, query: &str, body: Option<&[u8]>) -> Result<http::Response, Error> {
        let shard = self.topology.shard_for_key(key);
        let path = format!("/v1/kv/{}{query}", escape(key));
        let deadline = Instant::now() + self.timeout;
        let mut attempt = 0usize;
        let mut last = String::new();
        while Instant::now() < deadline {
            let base = self.leaders.borrow().get(&shard).cloned().unwrap_or_else(|| {
                let reps = self.topology.replicas(shard);
                self.topology.member(&reps[attempt % reps.len()]).map(|m| m.addr.clone()).unwrap_or_default()
            });
            let headers = [("X-Distkv-No-Forward", "1".to_string())];
            match http::request(method, &format!("{base}{path}"), &headers, body, self.timeout) {
                Ok(r) if r.status == 421 => {
                    let hint = json::parse(&r.text()).ok().map(|d| d.get("leader_addr").as_str().to_string()).unwrap_or_default();
                    if !hint.is_empty() && hint != base {
                        self.leaders.borrow_mut().insert(shard, hint);
                        continue;
                    }
                    self.leaders.borrow_mut().remove(&shard);
                    last = "misdirected".into();
                }
                Ok(r) if matches!(r.status, 502..=504) => {
                    self.leaders.borrow_mut().remove(&shard);
                    last = r.text();
                }
                Ok(r) => {
                    if r.status < 500 {
                        self.leaders.borrow_mut().insert(shard, base);
                    }
                    return Ok(r);
                }
                Err(e) => {
                    self.leaders.borrow_mut().remove(&shard);
                    last = e;
                }
            }
            attempt += 1;
            std::thread::sleep(Duration::from_millis(5u64 << attempt.min(6)));
        }
        Err(Error::Unavailable(last))
    }
}

fn escape(key: &str) -> String {
    let mut out = String::new();
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' | b':' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
