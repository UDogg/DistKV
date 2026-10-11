//go:build linux

package raft

import (
	"os"
	"syscall"
)

// fdatasync flushes file data (and only the metadata needed to read it
// back). Combined with preallocated segments it avoids journaling inode
// updates on every commit.
func fdatasync(f *os.File, noSync bool) error {
	if noSync {
		return nil
	}
	for {
		err := syscall.Fdatasync(int(f.Fd()))
		if err != syscall.EINTR {
			return err
		}
	}
}

// preallocate reserves size bytes of zeroed space for f (best effort).
func preallocate(f *os.File, size int64) {
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		// Unsupported filesystem: fall back to extending the file.
		if fi, statErr := f.Stat(); statErr == nil && fi.Size() < size {
			f.Truncate(size)
		}
	}
}
