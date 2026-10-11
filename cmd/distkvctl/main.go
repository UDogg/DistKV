// Command distkvctl is the DistKV command-line client.
//
//	distkvctl get user:42
//	distkvctl put user:42 alice --ttl 10m
//	distkvctl cas user:42 7 bob
//	distkvctl del user:42
//	distkvctl cluster
//	distkvctl transfer-leader 3 n2
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/UDogg/DistKV/internal/version"
	"github.com/UDogg/DistKV/pkg/client"
)

const usage = `distkvctl - DistKV command-line client

Usage:
  distkvctl [global flags] <command> [args]

Commands:
  get <key> [--stale]                   print a value (raw bytes to stdout)
  put <key> <value|-> [--ttl D]         write a value ('-' reads stdin)
  cas <key> <version> <value> [--ttl D] compare-and-set (version 0 = create)
  del <key>                             delete a key
  cluster [--json]                      show shard leaders and node health
  transfer-leader <shard> [<node>]      move a shard's leadership
  version                               print version

Global flags:
  --endpoints  comma-separated node URLs (env DISTKV_ENDPOINTS, default http://127.0.0.1:7001)
  --timeout    per-command timeout (default 5s)
  --no-smart   route through the endpoint instead of directly to leaders
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		if errors.Is(err, client.ErrNotFound) {
			os.Exit(4)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	global := flag.NewFlagSet("distkvctl", flag.ContinueOnError)
	global.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	defEndpoints := os.Getenv("DISTKV_ENDPOINTS")
	if defEndpoints == "" {
		defEndpoints = "http://127.0.0.1:7001"
	}
	endpoints := global.String("endpoints", defEndpoints, "")
	timeout := global.Duration("timeout", 5*time.Second, "")
	noSmart := global.Bool("no-smart", false, "")
	if err := global.Parse(args); err != nil {
		return err
	}
	rest := global.Args()
	if len(rest) == 0 {
		global.Usage()
		return errors.New("missing command")
	}
	cmd, rest := rest[0], rest[1:]
	if cmd == "version" {
		fmt.Println("distkvctl", version.String())
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	eps := strings.Split(*endpoints, ",")

	if cmd == "transfer-leader" {
		return transferLeader(ctx, eps, rest)
	}
	c, err := client.New(ctx, client.Config{Endpoints: eps, Timeout: *timeout, DisableSmartRouting: *noSmart})
	if err != nil {
		return err
	}
	defer c.Close()

	sub := flag.NewFlagSet(cmd, flag.ContinueOnError)
	ttl := sub.Duration("ttl", 0, "time to live")
	stale := sub.Bool("stale", false, "allow stale reads from any replica")
	asJSON := sub.Bool("json", false, "JSON output")
	pos, err := parseInterleaved(sub, rest)
	if err != nil {
		return err
	}
	var wopts []client.WriteOption
	if *ttl > 0 {
		wopts = append(wopts, client.WithTTL(*ttl))
	}

	switch cmd {
	case "get":
		if len(pos) != 1 {
			return errors.New("usage: get <key>")
		}
		var ropts []client.ReadOption
		if *stale {
			ropts = append(ropts, client.Stale())
		}
		it, err := c.Get(ctx, pos[0], ropts...)
		if err != nil {
			return err
		}
		os.Stdout.Write(it.Value)
		if isTerminal() {
			fmt.Println()
		}
		fmt.Fprintf(os.Stderr, "version=%d shard=%d served_by=%s", it.Version, it.Shard, it.ServedBy)
		if !it.ExpiresAt.IsZero() {
			fmt.Fprintf(os.Stderr, " expires_in=%v", time.Until(it.ExpiresAt).Round(time.Millisecond))
		}
		fmt.Fprintln(os.Stderr)
	case "put":
		if len(pos) != 2 {
			return errors.New("usage: put <key> <value|->")
		}
		val, err := valueArg(pos[1])
		if err != nil {
			return err
		}
		ver, err := c.Put(ctx, pos[0], val, wopts...)
		if err != nil {
			return err
		}
		fmt.Printf("OK version=%d\n", ver)
	case "cas":
		if len(pos) != 3 {
			return errors.New("usage: cas <key> <expected-version> <value|->")
		}
		expect, err := strconv.ParseUint(pos[1], 10, 64)
		if err != nil {
			return fmt.Errorf("bad version: %w", err)
		}
		val, err := valueArg(pos[2])
		if err != nil {
			return err
		}
		ver, err := c.CompareAndSwap(ctx, pos[0], expect, val, wopts...)
		if err != nil {
			return err
		}
		fmt.Printf("OK version=%d\n", ver)
	case "del", "delete":
		if len(pos) != 1 {
			return errors.New("usage: del <key>")
		}
		if err := c.Delete(ctx, pos[0]); err != nil {
			return err
		}
		fmt.Println("OK")
	case "cluster", "status":
		st, err := c.Cluster(ctx)
		if err != nil {
			return err
		}
		if *asJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(st)
		}
		fmt.Printf("cluster %s: %d nodes, %d shards (rf=%d), %d/%d shards healthy\n\n",
			st.Fingerprint, len(st.Nodes), st.Spec.Shards, st.Spec.ReplicationFactor, st.HealthyShards, len(st.Shards))
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "NODE\tADDRESS\tREACHABLE\tREADY\tLEADS\tHOSTS")
		for _, n := range st.Nodes {
			fmt.Fprintf(tw, "%s\t%s\t%v\t%v\t%d\t%d\n", n.ID, n.Addr, n.Reachable, n.Ready, n.ShardsLed, n.ShardsHeld)
		}
		tw.Flush()
		fmt.Println()
		tw = tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "SHARD\tLEADER\tTERM\tCOMMIT\tKEYS\tREPLICAS")
		for _, s := range st.Shards {
			leader := s.Leader
			if leader == "" {
				leader = "<none>"
			}
			fmt.Fprintf(tw, "%d\t%s\t%d\t%d\t%d\t%s\n", s.Shard, leader, s.Term, s.CommitIndex, s.Keys, strings.Join(s.Replicas, ","))
		}
		tw.Flush()
	default:
		global.Usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

func transferLeader(ctx context.Context, eps []string, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: transfer-leader <shard> [<node>]")
	}
	c, err := client.New(ctx, client.Config{Endpoints: eps})
	if err != nil {
		return err
	}
	st, err := c.Cluster(ctx)
	if err != nil {
		return err
	}
	shard, err := strconv.Atoi(args[0])
	if err != nil || shard < 0 || shard >= len(st.Shards) {
		return fmt.Errorf("invalid shard %q", args[0])
	}
	s := st.Shards[shard]
	if s.LeaderAddr == "" {
		return errors.New("shard currently has no leader")
	}
	u := fmt.Sprintf("%s/v1/admin/shards/%d/transfer-leader", s.LeaderAddr, shard)
	if len(args) > 1 {
		u += "?to=" + args[1]
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("transfer failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	fmt.Printf("shard %d: leadership moved from %s: %s", shard, s.Leader, body)
	return nil
}

// parseInterleaved allows flags after positional arguments.
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) > 0 {
			pos = append(pos, args[0])
			args = args[1:]
		}
	}
	return pos, nil
}

func valueArg(v string) ([]byte, error) {
	if v == "-" {
		return io.ReadAll(os.Stdin)
	}
	return []byte(v), nil
}

func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
