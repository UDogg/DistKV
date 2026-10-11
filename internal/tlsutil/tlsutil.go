// Package tlsutil builds TLS configurations for DistKV servers, peers and
// clients from PEM files, and can mint throwaway certificate authorities
// for tests and local development.
package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Files names PEM-encoded material.
type Files struct {
	CertFile string // certificate chain presented by this process
	KeyFile  string // private key for CertFile
	CAFile   string // CA bundle used to verify the other side
}

// Enabled reports whether any TLS material is configured.
func (f Files) Enabled() bool { return f.CertFile != "" || f.KeyFile != "" || f.CAFile != "" }

func (f Files) pool() (*x509.CertPool, error) {
	if f.CAFile == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(f.CAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tlsutil: no certificates found in %s", f.CAFile)
	}
	return pool, nil
}

// ServerConfig returns the listener configuration. When a CA is given,
// client certificates are verified if presented; requireClientCert makes
// them mandatory for every connection.
func ServerConfig(f Files, requireClientCert bool) (*tls.Config, error) {
	if f.CertFile == "" || f.KeyFile == "" {
		return nil, errors.New("tlsutil: server TLS needs a certificate and key")
	}
	cert, err := tls.LoadX509KeyPair(f.CertFile, f.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := f.pool()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: pool}
	switch {
	case requireClientCert && pool == nil:
		return nil, errors.New("tlsutil: requiring client certificates needs a CA file")
	case requireClientCert:
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	case pool != nil:
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return cfg, nil
}

// ClientConfig returns a configuration for dialing DistKV nodes. The
// certificate, if any, is presented for mutual TLS.
func ClientConfig(f Files) (*tls.Config, error) {
	pool, err := f.pool()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool}
	if f.CertFile != "" || f.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(f.CertFile, f.KeyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// GenerateDevPKI writes a throwaway CA plus a certificate valid for both
// server and client use (SANs: the given hosts, "localhost", 127.0.0.1) into
// dir as ca.pem, node.pem and node-key.pem. Intended for tests and local
// clusters only.
func GenerateDevPKI(dir string, hosts ...string) (Files, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Files{}, err
	}
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "DistKV development CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return Files{}, err
	}
	ca, _ := x509.ParseCertificate(caDER)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Files{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "distkv-node"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return Files{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return Files{}, err
	}
	f := Files{
		CAFile:   filepath.Join(dir, "ca.pem"),
		CertFile: filepath.Join(dir, "node.pem"),
		KeyFile:  filepath.Join(dir, "node-key.pem"),
	}
	writes := []struct {
		path, typ string
		der       []byte
		mode      os.FileMode
	}{
		{f.CAFile, "CERTIFICATE", caDER, 0o644},
		{f.CertFile, "CERTIFICATE", der, 0o644},
		{f.KeyFile, "EC PRIVATE KEY", keyDER, 0o600},
	}
	for _, w := range writes {
		if err := os.WriteFile(w.path, pem.EncodeToMemory(&pem.Block{Type: w.typ, Bytes: w.der}), w.mode); err != nil {
			return Files{}, err
		}
	}
	return f, nil
}
