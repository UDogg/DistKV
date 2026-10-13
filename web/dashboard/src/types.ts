// Wire types mirroring internal/api (Go).

export interface Load {
  reads_per_sec: number;
  writes_per_sec: number;
  errors_per_sec: number;
  forwarded_per_sec: number;
  p50_ms: number;
  p99_ms: number;
  p999_ms: number;
}

export interface Faults {
  isolated: boolean;
  drop_percent: number;
  delay_ms: number;
}

export interface NodeSummary {
  id: string;
  addr: string;
  reachable: boolean;
  ready: boolean;
  shards_led: number;
  shards_hosted: number;
  version?: string;
  uptime_seconds?: number;
  load?: Load;
  faults?: Faults;
}

export interface ShardSummary {
  shard: number;
  replicas: string[];
  leader?: string;
  leader_addr?: string;
  term: number;
  commit_index: number;
  keys: number;
  healthy: boolean;
  lag?: Record<string, number>;
}

export interface ClusterStatus {
  spec: {
    members: { id: string; addr: string }[];
    shards: number;
    replication_factor: number;
    virtual_nodes: number;
  };
  fingerprint: string;
  nodes: NodeSummary[];
  shards: ShardSummary[];
  healthy_shards: number;
  load: Load;
  ts: number;
  observed_by: string;
}

export type ProcState = "running" | "stopped" | "killed" | "exited";

export interface DemoNode {
  id: string;
  addr: string;
  pid: number;
  state: ProcState;
  since: string;
  starts: number;
}

export interface DemoInfo {
  demo: boolean;
  nodes: DemoNode[];
  rate: number;
  read_ratio: number;
  historian: boolean;
  shards: number;
  client?: ClientReport;
}

/** What the demo's traffic generator (a real retrying client) experienced. */
export interface ClientReport {
  ok_total: number;
  failed_total: number;
  availability: number;
  availability_60s: number;
  last_second: { ok: number; failed: number; p99_ms: number };
}

export interface KVResult {
  ok: boolean;
  status: number;
  ms: number;
  shard?: number;
  servedBy?: string;
  version?: number;
  leader?: string;
  body: string;
}

export interface HistoryPoint {
  t: number;
  reads: number;
  writes: number;
  errors: number;
  p50_ms: number;
  p99_ms: number;
  nodes_up: number;
  shards_healthy: number;
  shards_total: number;
}

export interface HistorianEvent {
  id: number;
  ts: number;
  kind: string;
  node?: string;
  shard?: number;
  detail: string;
}

export interface HistorianStats {
  db_bytes: number;
  samples: number;
  events: Record<string, number>;
  first_ts?: number;
}

export type ConnState = "live" | "reconnecting" | "sim";

/** Everything the UI needs from a cluster, live or simulated. */
export interface Backend {
  readonly mode: "live" | "sim";
  subscribe(onSnapshot: (s: ClusterStatus) => void, onConn: (c: ConnState) => void): void;
  demoInfo(): Promise<DemoInfo | null>;
  nodeAction(id: string, action: "kill" | "stop" | "start"): Promise<void>;
  setFaults(id: string, f: Faults): Promise<void>;
  setLoad(rate: number, readRatio: number): Promise<void>;
  kv(op: "put" | "get" | "delete", key: string, value?: string, ttlSec?: number): Promise<KVResult>;
  history(minutes: number): Promise<HistoryPoint[] | null>;
  historianEvents(): Promise<HistorianEvent[] | null>;
  historianStats(): Promise<HistorianStats | null>;
}
