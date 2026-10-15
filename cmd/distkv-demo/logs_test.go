package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReadLogIncremental(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.log")

	// Missing file: nothing yet, start from 0.
	lines, next, err := readLog(path, -1, 1024, 1024)
	if err != nil || len(lines) != 0 || next != 0 {
		t.Fatalf("missing file: %q %d %v", lines, next, err)
	}

	write := func(s string) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString(s)
		f.Close()
	}
	write("a\nb\nc")
	lines, next, _ = readLog(path, -1, 1024, 1024)
	if !reflect.DeepEqual(lines, []string{"a", "b"}) || next != 4 {
		t.Fatalf("first read: %q %d (an unfinished line must wait)", lines, next)
	}

	write("\nd\n")
	lines, next, _ = readLog(path, next, 1024, 1024)
	if !reflect.DeepEqual(lines, []string{"c", "d"}) || next != 8 {
		t.Fatalf("second read: %q %d", lines, next)
	}

	lines, next2, _ := readLog(path, next, 1024, 1024)
	if len(lines) != 0 || next2 != next {
		t.Fatalf("no new data: %q %d", lines, next2)
	}

	// Tail starting mid-line drops the fragment.
	lines, _, _ = readLog(path, -1, 5, 1024)
	if !reflect.DeepEqual(lines, []string{"c", "d"}) {
		t.Fatalf("tail: %q", lines)
	}

	// An offset past the end (file recreated) falls back to the tail.
	lines, _, _ = readLog(path, 1<<20, 1024, 1024)
	if len(lines) != 4 {
		t.Fatalf("stale offset: %q", lines)
	}

	// A line longer than the read limit is delivered in pieces, not stuck.
	os.WriteFile(path, []byte("0123456789\n"), 0o644)
	lines, next, _ = readLog(path, 0, 1024, 4)
	if !reflect.DeepEqual(lines, []string{"0123"}) || next != 4 {
		t.Fatalf("long line: %q %d", lines, next)
	}
}
