// Package forwarder implements Store & Forward's sending half: it reads the
// oldest pending chunks from the queue, sends them as one Message through
// a pluggable Adapter, and on the server's ack deletes them from the
// queue. It never blocks or is blocked by acquisition (Rule 1) — the two
// only share the queue.
package forwarder

import (
	"context"
)

// Adapter delivers one Message and returns nil only once the server has
// acknowledged it (MQTT: application-level ack; HTTP: a 2xx response).
// The transport sits behind this interface so it can be swapped
// (MQTTAdapter in production, HTTPAdapter for dev/test) without touching
// the forwarding loop.
type Adapter interface {
	Send(ctx context.Context, msg Message) error
}
