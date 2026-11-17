//go:build !unix

// Command distkv-study needs a Unix-like OS: its process faults are real
// signals (SIGKILL, SIGTERM, SIGSTOP, SIGCONT), which Windows doesn't have.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "distkv-study needs Linux, macOS or another Unix-like OS: its faults are POSIX signals (SIGSTOP and SIGCONT have no Windows equivalent).")
	os.Exit(2)
}
