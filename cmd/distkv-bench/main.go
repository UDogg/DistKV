// Command distkv-bench is an open-loop load generator for DistKV.
//
//	distkv-bench --endpoints http://127.0.0.1:7001 --rate 10000 --duration 30s
//
// It schedules operations at a constant rate and measures latency from each
// operation's scheduled start, so stalls show up in the tail percentiles
// instead of silently reducing the offered load.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/UDogg/DistKV/internal/loadgen"
	"github.com/UDogg/DistKV/internal/tlsutil"
	"github.com/UDogg/DistKV/pkg/client"
)

func main() {
	endpoints := flag.String("endpoints", "http://127.0.0.1:7001", "comma-separated node URLs")
	rate := flag.Int("rate", 10000, "target ops/s (0 = closed loop, max throughput)")
	duration := flag.Duration("duration", 30*time.Second, "measurement duration")
	warmup := flag.Duration("warmup", 3*time.Second, "unmeasured warmup before the measurement window")
	concurrency := flag.Int("concurrency", 256, "maximum in-flight operations")
	keys := flag.Int("keys", 100_000, "keyspace size")
	valueSize := flag.Int("value-size", 128, "value size in bytes")
	readRatio := flag.Float64("read-ratio", 0.8, "fraction of reads")
	stale := flag.Bool("stale", false, "serve reads from any replica")
	ttl := flag.Duration("ttl", 0, "TTL for written keys")
	preload := flag.Bool("preload", true, "write every key before measuring")
	noSmart := flag.Bool("no-smart", false, "send requests via the endpoints instead of directly to leaders")
	asJSON := flag.Bool("json", false, "emit a JSON summary")
	quiet := flag.Bool("quiet", false, "suppress per-second progress")
	var tlsFiles tlsutil.Files
	flag.StringVar(&tlsFiles.CAFile, "ca", "", "TLS CA bundle")
	flag.StringVar(&tlsFiles.CertFile, "cert", "", "TLS client certificate (mutual TLS)")
	flag.StringVar(&tlsFiles.KeyFile, "key", "", "TLS client key")
	flag.Parse()

	var tlsCfg *tls.Config
	if tlsFiles.Enabled() {
		var err error
		if tlsCfg, err = tlsutil.ClientConfig(tlsFiles); err != nil {
			fmt.Fprintln(os.Stderr, "tls:", err)
			os.Exit(1)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	setup, setupCancel := context.WithTimeout(ctx, 10*time.Second)
	c, err := client.New(setup, client.Config{Endpoints: strings.Split(*endpoints, ","), DisableSmartRouting: *noSmart, TLS: tlsCfg})
	setupCancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer c.Close()

	cfg := loadgen.Config{
		Rate: *rate, Duration: *duration, Warmup: *warmup, Concurrency: *concurrency, Keys: *keys, ValueSize: *valueSize,
		ReadRatio: *readRatio, StaleReads: *stale, TTL: *ttl,
	}
	if !*quiet && !*asJSON {
		cfg.OnSecond = func(s loadgen.Second) {
			fmt.Fprintf(os.Stderr, "[%4s] %7d ops/s  errors %-5d p99 %v\n", s.Elapsed, s.Ops, s.Errors, s.P99)
		}
	}
	if *preload {
		fmt.Fprintf(os.Stderr, "preloading %d keys...\n", *keys)
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Minute)
		err := loadgen.Preload(pctx, c, cfg)
		pcancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, "preload:", err)
			os.Exit(1)
		}
	}
	rep := loadgen.Run(ctx, c, cfg)
	if *asJSON {
		out := map[string]any{
			"duration_s":          rep.Elapsed.Seconds(),
			"target_rate":         *rate,
			"ops":                 rep.Ops,
			"errors":              rep.Errors,
			"throughput":          rep.Throughput(),
			"availability":        rep.Availability(),
			"worst_second_avail":  rep.WorstSecondAvailability(),
			"p50_ms":              ms(rep.All.Quantile(0.5)),
			"p90_ms":              ms(rep.All.Quantile(0.9)),
			"p99_ms":              ms(rep.All.Quantile(0.99)),
			"p999_ms":             ms(rep.All.Quantile(0.999)),
			"max_ms":              ms(rep.All.Max()),
			"read_p99_ms":         ms(rep.Reads.Quantile(0.99)),
			"write_p99_ms":        ms(rep.Writes.Quantile(0.99)),
			"sample_errors":       rep.ErrorSamples,
			"per_second_timeline": rep.Timeline,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(out)
		return
	}
	fmt.Print("\n" + rep.Format())
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
