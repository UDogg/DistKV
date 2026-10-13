// Command distkv runs a DistKV server node.
//
// Example three-node cluster on one machine:
//
//	distkv --id n1 --listen :7001 --data-dir /tmp/dkv/n1 \
//	  --members n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003
//
// All flags can also be supplied through a JSON file (--config) or
// environment variables named DISTKV_<FLAG> (e.g. DISTKV_MEMBERS).
// Precedence: flags > environment > config file > defaults.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/server"
	"github.com/UDogg/DistKV/internal/tlsutil"
	"github.com/UDogg/DistKV/internal/version"
)

type options struct {
	ID                string
	Listen            string
	Members           string
	DataDir           string
	InMemory          bool
	NoSync            bool
	Shards            int
	ReplicationFactor int
	VirtualNodes      int
	Heartbeat         time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold uint64
	ReadMode          string
	MaxKeysPerShard   int
	MaxBytesPerShard  int64
	RequestTimeout    time.Duration
	Rebalance         time.Duration
	ShutdownTimeout   time.Duration
	LogLevel          string
	LogFormat         string
	Pprof             bool
	TLSCert           string
	TLSKey            string
	TLSCA             string
	TLSClientAuth     bool
	FaultInjection    bool
	CORSOrigin        string
	DisableUI         bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "distkv:", err)
		os.Exit(1)
	}
}

func run() error {
	o := options{}
	fs := flag.NewFlagSet("distkv", flag.ExitOnError)
	configPath := fs.String("config", "", "path to a JSON config file (flags override it)")
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.StringVar(&o.ID, "id", "", "this node's ID (must appear in --members)")
	fs.StringVar(&o.Listen, "listen", "", "listen address (default: the port of this node's member address)")
	fs.StringVar(&o.Members, "members", "", "comma-separated id=host:port list of ALL cluster members")
	fs.StringVar(&o.DataDir, "data-dir", "", "directory for Raft logs and snapshots")
	fs.BoolVar(&o.InMemory, "in-memory", false, "keep all state in memory (dev/test only: unsafe across restarts)")
	fs.BoolVar(&o.NoSync, "no-sync", false, "skip fsync (durable across process crashes, not power loss)")
	fs.IntVar(&o.Shards, "shards", 16, "number of shards (Raft groups); fixed for the life of the cluster")
	fs.IntVar(&o.ReplicationFactor, "replication", 3, "replicas per shard")
	fs.IntVar(&o.VirtualNodes, "vnodes", cluster.DefaultVirtualNodes, "consistent-hash ring points per shard")
	fs.DurationVar(&o.Heartbeat, "heartbeat", 50*time.Millisecond, "Raft heartbeat interval")
	fs.DurationVar(&o.ElectionTimeout, "election-timeout", 500*time.Millisecond, "Raft base election timeout")
	fs.Uint64Var(&o.SnapshotThreshold, "snapshot-threshold", 20000, "applied entries between snapshots")
	fs.StringVar(&o.ReadMode, "read-mode", server.ReadModeLease, "linearizable read strategy: lease | readindex")
	fs.IntVar(&o.MaxKeysPerShard, "max-keys-per-shard", 0, "LRU capacity per shard in keys (0 = unlimited)")
	fs.Int64Var(&o.MaxBytesPerShard, "max-bytes-per-shard", 0, "LRU capacity per shard in bytes (0 = unlimited)")
	fs.DurationVar(&o.RequestTimeout, "request-timeout", 3*time.Second, "end-to-end client request timeout")
	fs.DurationVar(&o.Rebalance, "rebalance-interval", 5*time.Second, "leader balancing interval (0 disables)")
	fs.DurationVar(&o.ShutdownTimeout, "shutdown-timeout", 15*time.Second, "graceful shutdown budget")
	fs.StringVar(&o.LogLevel, "log-level", "info", "debug | info | warn | error")
	fs.StringVar(&o.LogFormat, "log-format", "text", "text | json")
	fs.BoolVar(&o.Pprof, "pprof", false, "expose /debug/pprof")
	fs.BoolVar(&o.FaultInjection, "enable-fault-injection", false, "expose /v1/admin/faults (partition/drop/delay injection; demos only)")
	fs.StringVar(&o.CORSOrigin, "cors-origin", "", "Access-Control-Allow-Origin for browser dashboards on other origins")
	fs.BoolVar(&o.DisableUI, "disable-ui", false, "do not serve the embedded dashboard at /ui/")
	fs.StringVar(&o.TLSCert, "tls-cert", "", "PEM certificate for HTTPS (also presented to peers)")
	fs.StringVar(&o.TLSKey, "tls-key", "", "PEM private key for --tls-cert")
	fs.StringVar(&o.TLSCA, "tls-ca", "", "PEM CA bundle: verifies peers and enables mutual TLS for Raft/admin endpoints")
	fs.BoolVar(&o.TLSClientAuth, "tls-client-auth", false, "require client certificates on every connection (incl. the KV API)")

	// Config file first, then environment, then flags.
	_ = fs.Parse(os.Args[1:])
	if *showVersion {
		fmt.Println("distkv", version.String())
		return nil
	}
	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return err
		}
		// The file is a flat object keyed by flag name, e.g.
		// {"id": "n1", "election-timeout": "500ms", "shards": 16}.
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("parsing %s: %w", *configPath, err)
		}
		for k, v := range raw {
			if err := fs.Set(k, fmt.Sprint(v)); err != nil {
				return fmt.Errorf("%s: %s: %w", *configPath, k, err)
			}
		}
	}
	fs.VisitAll(func(f *flag.Flag) {
		env := "DISTKV_" + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		if v, ok := os.LookupEnv(env); ok {
			fs.Set(f.Name, v)
		}
	})
	_ = fs.Parse(os.Args[1:]) // explicit flags win

	logger, err := newLogger(o.LogLevel, o.LogFormat)
	if err != nil {
		return err
	}
	if o.ID == "" || o.Members == "" {
		return errors.New("--id and --members are required")
	}
	if o.DataDir == "" && !o.InMemory {
		return errors.New("--data-dir is required (or pass --in-memory for a throwaway node)")
	}
	members, err := cluster.ParseMembers(o.Members)
	if err != nil {
		return err
	}
	listen := o.Listen
	if listen == "" {
		for _, m := range members {
			if string(m.ID) == o.ID {
				u, err := url.Parse(m.Addr)
				if err != nil {
					return err
				}
				_, port, _ := net.SplitHostPort(u.Host)
				listen = ":" + port
			}
		}
	}
	dataDir := o.DataDir
	if o.InMemory {
		dataDir = ""
	}

	var serverTLS, peerTLS *tls.Config
	if files := (tlsutil.Files{CertFile: o.TLSCert, KeyFile: o.TLSKey, CAFile: o.TLSCA}); files.Enabled() {
		if serverTLS, err = tlsutil.ServerConfig(files, o.TLSClientAuth); err != nil {
			return err
		}
		if peerTLS, err = tlsutil.ClientConfig(files); err != nil {
			return err
		}
		for _, m := range members {
			if !strings.HasPrefix(m.Addr, "https://") {
				return fmt.Errorf("TLS is enabled but member %s has a non-https address %q", m.ID, m.Addr)
			}
		}
	}

	srv, err := server.New(server.Config{
		NodeID: cluster.NodeID(o.ID),
		Spec: cluster.Spec{
			Members:           members,
			Shards:            o.Shards,
			ReplicationFactor: o.ReplicationFactor,
			VirtualNodes:      o.VirtualNodes,
		},
		DataDir:              dataDir,
		NoSync:               o.NoSync,
		HeartbeatInterval:    o.Heartbeat,
		ElectionTimeout:      o.ElectionTimeout,
		SnapshotThreshold:    o.SnapshotThreshold,
		ReadMode:             o.ReadMode,
		MaxKeysPerShard:      o.MaxKeysPerShard,
		MaxBytesPerShard:     o.MaxBytesPerShard,
		RequestTimeout:       o.RequestTimeout,
		RebalanceInterval:    o.Rebalance,
		EnablePprof:          o.Pprof,
		EnableFaultInjection: o.FaultInjection,
		CORSOrigin:           o.CORSOrigin,
		DisableUI:            o.DisableUI,
		TLS:                  serverTLS,
		PeerTLS:              peerTLS,
		Logger:               logger,
	})
	if err != nil {
		return err
	}
	logger.Info("starting distkv", "version", version.String(), "id", o.ID, "listen", listen,
		"members", len(members), "shards", o.Shards, "replication", o.ReplicationFactor, "data_dir", dataDir)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(listen) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		return err
	case s := <-sig:
		logger.Info("received signal; shutting down gracefully", "signal", s.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), o.ShutdownTimeout)
	defer cancel()
	go func() {
		<-sig
		logger.Warn("second signal: exiting immediately")
		os.Exit(2)
	}()
	return srv.Shutdown(ctx)
}

func newLogger(level, format string) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid --log-level %q", level)
	}
	opts := &slog.HandlerOptions{Level: l}
	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("invalid --log-format %q", format)
	}
}
