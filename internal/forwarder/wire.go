package forwarder

import (
	"nxiiot-gateway/internal/queue"
)

// Message is the JSON body sent to the server (MQTT data topic payload or
// HTTP POST body) — the V2 wire contract, see Server_Design_Spec.md.
// It carries one or more whole queue chunks in seq order. The server
// de-duplicates on (gateway_id, stream_id, seq): a message may be
// re-delivered after a lost or late ack. A gap in seq between messages
// of the same stream means the gateway evicted that data (queue full).
type Message struct {
	GatewayID string      `json:"gateway_id"`
	StreamID  string      `json:"stream_id"`
	FromSeq   int64       `json:"from_seq"`
	ToSeq     int64       `json:"to_seq"`
	Chunks    []WireChunk `json:"chunks"`
}

// WireChunk is one queue chunk: the readings from one flush interval.
type WireChunk struct {
	Seq      int64         `json:"seq"`
	Readings []WireReading `json:"readings"`
}

// WireReading is one acquired value. Value is null whenever Quality is not
// GOOD.
type WireReading struct {
	DeviceID    int64    `json:"device_id"`
	DatapointID int64    `json:"datapoint_id"`
	Timestamp   string   `json:"ts"` // RFC 3339, UTC, millisecond precision
	Value       *float64 `json:"value"`
	Quality     string   `json:"quality"`
}

// Ack is what the server publishes on the ack topic once it has stored a
// message (or rejected it, with Error set). ToSeq echoes the message's
// to_seq; StreamID is optional and only checked when present.
type Ack struct {
	StreamID string `json:"stream_id,omitempty"`
	ToSeq    int64  `json:"to_seq"`
	Error    string `json:"error,omitempty"`
}

const wireTimeLayout = "2006-01-02T15:04:05.000Z07:00"

// buildMessage converts queue chunks (non-empty, in seq order) to a Message.
func buildMessage(gatewayID, streamID string, chunks []queue.Chunk) Message {
	msg := Message{
		GatewayID: gatewayID,
		StreamID:  streamID,
		FromSeq:   chunks[0].Seq,
		ToSeq:     chunks[len(chunks)-1].Seq,
		Chunks:    make([]WireChunk, len(chunks)),
	}
	for i, c := range chunks {
		wc := WireChunk{Seq: c.Seq, Readings: make([]WireReading, len(c.Readings))}
		for j, r := range c.Readings {
			wc.Readings[j] = WireReading{
				DeviceID:    r.DeviceID,
				DatapointID: r.DatapointID,
				Timestamp:   r.At.UTC().Format(wireTimeLayout),
				Value:       r.Value,
				Quality:     r.Quality,
			}
		}
		msg.Chunks[i] = wc
	}
	return msg
}

// ReadingCount is the number of readings in m.
func (m Message) ReadingCount() int {
	n := 0
	for _, c := range m.Chunks {
		n += len(c.Readings)
	}
	return n
}
