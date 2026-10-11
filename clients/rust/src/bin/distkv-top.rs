//! distkv-top: an htop-style live terminal view of a DistKV cluster.
//!
//!     distkv-top [--endpoints http://127.0.0.1:7001,...] [--interval 1] [--once]

use distkv::json::Value;
use std::collections::VecDeque;
use std::time::Duration;

const COLORS: [u8; 9] = [99, 45, 205, 48, 214, 69, 171, 87, 209]; // 256-colour palette per node
const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const RED: &str = "\x1b[38;5;203m";
const GREEN: &str = "\x1b[38;5;48m";
const YELLOW: &str = "\x1b[38;5;214m";

fn fg(c: u8) -> String {
    format!("\x1b[38;5;{c}m")
}
fn bg(c: u8) -> String {
    format!("\x1b[48;5;{c}m\x1b[38;5;16m")
}

fn spark(values: &VecDeque<f64>, width: usize) -> String {
    const BARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let max = values.iter().cloned().fold(1.0, f64::max);
    let start = values.len().saturating_sub(width);
    values.iter().skip(start).map(|v| BARS[((v / max) * 7.0).round().clamp(0.0, 7.0) as usize]).collect()
}

fn fmt_ms(v: f64) -> String {
    if v <= 0.0 {
        "—".into()
    } else if v >= 1000.0 {
        format!("{:.2}s", v / 1000.0)
    } else if v >= 10.0 {
        format!("{v:.0}ms")
    } else {
        format!("{v:.2}ms")
    }
}

fn render(st: &Value, ops_hist: &VecDeque<f64>, p99_hist: &VecDeque<f64>) -> String {
    let mut out = String::new();
    let members: Vec<&str> = st.get("spec").get("members").as_array().iter().map(|m| m.get("id").as_str()).collect();
    let color = |id: &str| COLORS[members.iter().position(|m| *m == id).unwrap_or(0) % COLORS.len()];
    let load = st.get("load");
    let ops = load.get("reads_per_sec").as_f64() + load.get("writes_per_sec").as_f64();
    let shards = st.get("shards").as_array();
    let healthy = st.get("healthy_shards").as_u64() as usize;
    let nodes = st.get("nodes").as_array();
    let up = nodes.iter().filter(|n| n.get("reachable").as_bool()).count();

    let status = if healthy == shards.len() && up == nodes.len() {
        format!("{GREEN}{BOLD}● HEALTHY{RESET}")
    } else if healthy == shards.len() {
        format!("{YELLOW}{BOLD}● DEGRADED{RESET}")
    } else {
        format!("{RED}{BOLD}● FAILOVER ({} shards without leader){RESET}", shards.len() - healthy)
    };
    out += &format!(
        "{BOLD}{}DistKV{RESET}  {status}   {DIM}cluster {} · via {}{RESET}\n\n",
        fg(99),
        st.get("fingerprint").as_str(),
        st.get("observed_by").as_str()
    );
    out += &format!(
        "  {BOLD}{:>8.0}{RESET} ops/s  {}{}{RESET}   p99 {BOLD}{:>8}{RESET} {}{}{RESET}   shards {BOLD}{healthy}/{}{RESET}   nodes {BOLD}{up}/{}{RESET}\n\n",
        ops,
        fg(45),
        spark(ops_hist, 30),
        fmt_ms(load.get("p99_ms").as_f64()),
        fg(214),
        spark(p99_hist, 20),
        shards.len(),
        nodes.len()
    );

    out += &format!("  {BOLD}{:<6} {:<24} {:<10} {:>5} {:>9} {:>9}  {:<20}{RESET}\n", "NODE", "ADDRESS", "STATE", "LEADS", "OPS/S", "P99", "LEADERSHIP");
    let max_lead = nodes.iter().map(|n| n.get("shards_led").as_u64()).max().unwrap_or(1).max(1);
    for n in nodes {
        let id = n.get("id").as_str();
        let c = color(id);
        let reachable = n.get("reachable").as_bool();
        let isolated = n.get("faults").get("isolated").as_bool();
        let state = if !reachable {
            format!("{RED}down      {RESET}")
        } else if isolated {
            format!("{YELLOW}partitioned{RESET}")
        } else if n.get("ready").as_bool() {
            format!("{GREEN}ready     {RESET}")
        } else {
            format!("{YELLOW}degraded  {RESET}")
        };
        let l = n.get("load");
        let led = n.get("shards_led").as_u64();
        let bar = "█".repeat(((led as f64 / max_lead as f64) * 20.0).round() as usize);
        out += &format!(
            "  {}{BOLD}{:<6}{RESET} {:<24} {} {:>5} {:>9.0} {:>9}  {}{bar}{RESET}\n",
            fg(c),
            id,
            n.get("addr").as_str().trim_start_matches("http://"),
            state,
            led,
            l.get("reads_per_sec").as_f64() + l.get("writes_per_sec").as_f64(),
            fmt_ms(l.get("p99_ms").as_f64()),
            fg(c),
        );
    }

    out += &format!("\n  {BOLD}SHARD LEADERS{RESET} {DIM}(each cell is a shard, coloured by its leader){RESET}\n  ");
    for (i, s) in shards.iter().enumerate() {
        let leader = s.get("leader").as_str();
        if leader.is_empty() {
            out += &format!("{RED}{BOLD} ✕{:<2}{RESET}", s.get("shard").as_u64());
        } else {
            out += &format!("{} {:<3}{RESET}", bg(color(leader)), s.get("shard").as_u64());
        }
        out.push(' ');
        if (i + 1) % 16 == 0 {
            out += "\n  ";
        }
    }
    out += &format!("\n\n  {DIM}");
    for m in &members {
        out += &format!("{}■{RESET}{DIM} {m}  ", fg(color(m)));
    }
    out += &format!("· q to quit (Ctrl-C){RESET}\n");
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut endpoints = std::env::var("DISTKV_ENDPOINTS").unwrap_or_else(|_| "http://127.0.0.1:7001".into());
    let mut interval = 1.0f64;
    let mut once = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--endpoints" | "-e" => {
                endpoints = args.get(i + 1).cloned().unwrap_or_default();
                i += 1;
            }
            "--interval" | "-n" => {
                interval = args.get(i + 1).and_then(|v| v.parse().ok()).unwrap_or(1.0);
                i += 1;
            }
            "--once" => once = true,
            "-h" | "--help" => {
                println!("distkv-top [--endpoints URL,...] [--interval SECONDS] [--once]");
                return;
            }
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    let eps: Vec<&str> = endpoints.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
    let mut ops_hist = VecDeque::new();
    let mut p99_hist = VecDeque::new();
    loop {
        match distkv::cluster_status(&eps, Duration::from_secs(2)) {
            Ok(st) => {
                let load = st.get("load");
                ops_hist.push_back(load.get("reads_per_sec").as_f64() + load.get("writes_per_sec").as_f64());
                p99_hist.push_back(load.get("p99_ms").as_f64());
                if ops_hist.len() > 60 {
                    ops_hist.pop_front();
                    p99_hist.pop_front();
                }
                let frame = render(&st, &ops_hist, &p99_hist);
                if once {
                    print!("{frame}");
                    return;
                }
                print!("\x1b[2J\x1b[H{frame}");
            }
            Err(e) => {
                if once {
                    eprintln!("{e}");
                    std::process::exit(1);
                }
                println!("\x1b[2J\x1b[H{RED}cannot reach cluster: {e}{RESET}");
            }
        }
        std::thread::sleep(Duration::from_secs_f64(interval));
    }
}
