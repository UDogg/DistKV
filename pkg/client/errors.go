package client

import (
	"errors"
	"net"
)

// isDialError reports whether err occurred before the request was sent.
func isDialError(err error) bool {
	var oe *net.OpError
	return errors.As(err, &oe) && oe.Op == "dial"
}
