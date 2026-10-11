package server_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/testcluster"
	"github.com/UDogg/DistKV/internal/tlsutil"
	"github.com/UDogg/DistKV/internal/transport"
	"github.com/UDogg/DistKV/pkg/client"
)

func TestTLSCluster(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 4, TLS: true})
	for _, ep := range tc.Endpoints() {
		if !strings.HasPrefix(ep, "https://") {
			t.Fatalf("endpoint %s is not https", ep)
		}
	}

	// Raft replication happened over mutual TLS (the cluster elected
	// leaders); a CA-verifying client without a certificate can use the
	// KV API.
	c := tc.Client()
	if _, err := c.Put(ctx(t), "secret", []byte("s3cr3t")); err != nil {
		t.Fatal(err)
	}
	if it, err := c.Get(ctx(t), "secret"); err != nil || string(it.Value) != "s3cr3t" {
		t.Fatalf("get over TLS: %v", err)
	}

	caOnly, err := tlsutil.ClientConfig(tlsutil.Files{CAFile: tc.TLSFiles.CAFile})
	if err != nil {
		t.Fatal(err)
	}
	anon := transport.NewHTTPClient(caOnly)
	ep := tc.Endpoints()[0]

	// Raft and admin endpoints refuse callers without a cluster certificate.
	for _, path := range []string{"/internal/raft/0/vote", "/v1/admin/shards/0/transfer-leader"} {
		resp, err := anon.Post(ep+path, "application/octet-stream", strings.NewReader("x"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("POST %s without client cert: %d, want 403", path, resp.StatusCode)
		}
	}

	// Plain HTTP against a TLS listener fails.
	if resp, err := http.Get(strings.Replace(ep, "https://", "http://", 1) + "/healthz"); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("plaintext request succeeded against TLS listener")
		}
	}

	// A client that does not trust the cluster CA is rejected.
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	untrusted := &tls.Config{MinVersion: tls.VersionTLS13}
	if _, err := client.New(ctx2, client.Config{Endpoints: tc.Endpoints(), TLS: untrusted}); err == nil {
		t.Errorf("client without the cluster CA connected")
	}
}
