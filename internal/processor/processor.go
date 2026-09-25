// Package processor sits between acquisition and storage: it takes each
// Reading produced by the Modbus engine (which already carries its
// event timestamp and quality — Design Principle #6) and hands it to the
// Store & Forward queue.
package processor

import (
	"nxiiot-gateway/internal/acquisition"
	"nxiiot-gateway/internal/queue"
)

// Appender is the queue as the processor sees it (*queue.Store).
type Appender interface {
	Append(queue.Reading)
}

type Processor struct {
	q Appender
}

func New(q Appender) *Processor {
	return &Processor{q: q}
}

// Process queues one acquired Reading. It never mutates r.EventTimestamp
// (Rule 5) and queues readings of every quality, not just GOOD — a failed
// read is itself meaningful history. Append only buffers in memory; the
// queue writes to disk on its own flush interval, so this never blocks the
// poller on I/O.
func (p *Processor) Process(r acquisition.Reading) {
	p.q.Append(queue.Reading{
		DeviceID:    r.DeviceID,
		DatapointID: r.DatapointID,
		Value:       r.Value,
		Quality:     string(r.Quality),
		At:          r.EventTimestamp,
	})
}
