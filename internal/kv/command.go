// Package kv implements DistKV's replicated state machine: an in-memory
// cache with per-key TTLs, bounded capacity with LRU eviction, optimistic
// concurrency (compare-and-set on versions) and exactly-once semantics for
// retried client requests.
//
// Every mutation flows through the Raft log, so Apply must be fully
// deterministic: wall-clock time is never read during Apply. Instead the
// proposing leader stamps each command with its clock (Command.Now), and
// the store advances a logical clock that is identical on every replica.
package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Op is a state machine operation.
type Op uint8

const (
	OpPut Op = iota + 1
	OpDelete
	// OpCAS writes only if the key's current version equals
	// Command.ExpectVersion (0 means "key must not exist").
	OpCAS
	// OpNoop advances the logical clock (used to expire keys on idle shards)
	OpNoop
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpCAS:
		return "cas"
	case OpNoop:
		return "noop"
	default:
		return fmt.Sprintf("op(%d)", uint8(o))
	}
}

// Command is a replicated mutation.
type Command struct {
	Op  Op
	Key string
	// Value is the payload for OpPut/OpCAS.
	Value []byte
	// ExpireAt is an absolute deadline in Unix nanoseconds, or 0.
	ExpireAt int64
	// ExpectVersion is the required current version for OpCAS.
	ExpectVersion uint64
	// Now is the proposing leader's wall clock in Unix nanoseconds.
	Now int64

	// Exactly-once session fields. ClientID 0 disables deduplication.
	ClientID uint64
	Seq      uint64
	// AckBelow tells the server that the client has received responses
	// for every Seq < AckBelow, allowing their cached results to be freed.
	AckBelow uint64
}

const commandVersion = 1

// ErrBadCommand is returned when a command cannot be decoded.
var ErrBadCommand = errors.New("kv: malformed command")

// Encode serialises c into a compact binary form.
func (c *Command) Encode() []byte {
	b := make([]byte, 0, 64+len(c.Key)+len(c.Value))
	b = append(b, commandVersion, byte(c.Op))
	b = binary.AppendUvarint(b, c.ClientID)
	b = binary.AppendUvarint(b, c.Seq)
	b = binary.AppendUvarint(b, c.AckBelow)
	b = binary.AppendVarint(b, c.Now)
	b = binary.AppendVarint(b, c.ExpireAt)
	b = binary.AppendUvarint(b, c.ExpectVersion)
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	b = binary.AppendUvarint(b, uint64(len(c.Value)))
	return append(b, c.Value...)
}

// DecodeCommand parses the output of Encode.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	if len(b) < 2 || b[0] != commandVersion {
		return c, ErrBadCommand
	}
	c.Op = Op(b[1])
	d := decoder{b: b[2:]}
	c.ClientID = d.uvarint()
	c.Seq = d.uvarint()
	c.AckBelow = d.uvarint()
	c.Now = d.varint()
	c.ExpireAt = d.varint()
	c.ExpectVersion = d.uvarint()
	c.Key = string(d.bytes())
	if v := d.bytes(); len(v) > 0 {
		c.Value = append([]byte(nil), v...)
	}
	if d.err || len(d.b) != 0 {
		return Command{}, ErrBadCommand
	}
	return c, nil
}

type decoder struct {
	b   []byte
	err bool
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = true
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = true
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err || n > uint64(len(d.b)) {
		d.err = true
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}
