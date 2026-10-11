//go:build linux

package main

import "syscall"

// childAttrs makes node processes receive SIGTERM if the supervisor dies,
// so a crashed demo never leaves orphaned servers behind.
func childAttrs() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}
