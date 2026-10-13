//go:build !linux

package raft

import "os"

func fdatasync(f *os.File, noSync bool) error {
	if noSync {
		return nil
	}
	return f.Sync()
}

func preallocate(f *os.File, size int64) {
	if fi, err := f.Stat(); err == nil && fi.Size() < size {
		f.Truncate(size)
	}
}
