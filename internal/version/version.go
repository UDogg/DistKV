// Package version carries build metadata injected via -ldflags.
package version

import "runtime"

// Set at build time:
//
//	go build -ldflags "-X github.com/UDogg/DistKV/internal/version.Version=v1.0.0 \
//	  -X github.com/UDogg/DistKV/internal/version.Commit=$(git rev-parse --short HEAD)"
var (
	Version = "dev"
	Commit  = "unknown"
)

// String returns a human-readable version line.
func String() string {
	return Version + " (" + Commit + ", " + runtime.Version() + ")"
}
