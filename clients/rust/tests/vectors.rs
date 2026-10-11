//! Routing parity with the Go servers (golden vectors).

use distkv::{hash64, Member, Topology};

fn vectors() -> Vec<Vec<String>> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/routing/vectors.tsv");
    std::fs::read_to_string(path)
        .expect("vectors.tsv")
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| l.split('\t').map(String::from).collect())
        .collect()
}

fn topology() -> Topology {
    let members = (1..=5).rev().map(|i| Member { id: format!("n{i}"), addr: format!("http://127.0.0.1:{}", 7100 + i) }).collect();
    Topology::new(members, 24, 3, 128).unwrap()
}

#[test]
fn hash64_matches_go() {
    let v = vectors();
    let rows: Vec<_> = v.iter().filter(|r| r[0] == "hash").collect();
    assert!(rows.len() > 200);
    for r in rows {
        assert_eq!(hash64(&r[1]), r[2].parse::<u64>().unwrap(), "key {:?}", r[1]);
    }
}

#[test]
fn shard_for_key_matches_go() {
    let t = topology();
    for r in vectors().iter().filter(|r| r[0] == "shard") {
        assert_eq!(t.shard_for_key(&r[1]), r[2].parse::<usize>().unwrap(), "key {:?}", r[1]);
    }
}

#[test]
fn replicas_match_go() {
    let t = topology();
    for r in vectors().iter().filter(|r| r[0] == "replicas") {
        let want: Vec<String> = r[2].split(',').map(String::from).collect();
        assert_eq!(t.replicas(r[1].parse().unwrap()), want.as_slice(), "shard {}", r[1]);
    }
}

#[test]
#[ignore = "needs a live cluster: DISTKV_ENDPOINTS=... cargo test -- --ignored"]
fn live_cluster_round_trip() {
    let eps = std::env::var("DISTKV_ENDPOINTS").unwrap_or_else(|_| "http://127.0.0.1:7001".into());
    let eps: Vec<&str> = eps.split(',').collect();
    let c = distkv::Client::connect(&eps).unwrap();
    for i in 0..30 {
        c.put(&format!("rs:{i}"), format!("v{i}").as_bytes(), None).unwrap();
    }
    for i in 0..30 {
        let it = c.get(&format!("rs:{i}")).unwrap().unwrap();
        assert_eq!(it.value, format!("v{i}").as_bytes());
        assert_eq!(it.shard, c.topology.shard_for_key(&format!("rs:{i}")), "routing must match the server");
    }
    let v = c.get("rs:0").unwrap().unwrap().version;
    assert!(matches!(c.compare_and_swap("rs:0", v + 999, b"x"), Err(distkv::Error::Conflict { .. })));
}
