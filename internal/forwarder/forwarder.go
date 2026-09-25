package forwarder

import (
	"context"
	"log/slog"
	"time"

	"nxiiot-gateway/internal/queue"
)

// Source is the queue as the forwarder sees it (*queue.Store).
type Source interface {
	StreamID() string
	ReadBatch(ctx context.Context, maxReadings int) ([]queue.Chunk, error)
	Ack(ctx context.Context, throughSeq int64) error
}

type Config struct {
	GatewayID string
	// MaxReadingsPerMessage caps one message; whole chunks are added until
	// it is reached, so a message can exceed it by at most one chunk.
	MaxReadingsPerMessage int
	// PollInterval is how long to wait for new data once the queue is
	// drained. While a backlog exists messages are sent back to back.
	PollInterval time.Duration
}

type Forwarder struct {
	src     Source
	adapter Adapter
	cfg     Config
	log     *slog.Logger
	status  statusTracker
}

func New(src Source, adapter Adapter, cfg Config, log *slog.Logger) *Forwarder {
	if cfg.MaxReadingsPerMessage <= 0 {
		cfg.MaxReadingsPerMessage = 2000
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	return &Forwarder{src: src, adapter: adapter, cfg: cfg, log: log}
}

// Status reports current server connectivity, for the Store & Forward API.
func (f *Forwarder) Status() Status {
	return f.status.get()
}

// Run sends until ctx is cancelled. After a failure it waits
// BackoffDuration (1, 2, 4 … 60 s) and resends the same oldest data, so
// delivery is at-least-once and in seq order. Nothing is marked in the
// queue before the ack, so a crash mid-send needs no recovery step (V1's
// SENDING state did).
func (f *Forwarder) Run(ctx context.Context) {
	failures := 0
	for {
		sent, err := f.dispatchOnce(ctx)
		var wait time.Duration
		switch {
		case err != nil:
			failures++
			wait = BackoffDuration(failures)
		case sent:
			failures = 0
			// backlog: go straight on to the next message
		default:
			failures = 0
			wait = f.cfg.PollInterval
		}

		if wait == 0 {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// dispatchOnce sends one message; sent reports whether there was anything
// to send and it was acknowledged.
func (f *Forwarder) dispatchOnce(ctx context.Context) (sent bool, err error) {
	chunks, err := f.src.ReadBatch(ctx, f.cfg.MaxReadingsPerMessage)
	if err != nil {
		f.log.Error("failed to read queue for forwarding", "error", err)
		return false, err
	}
	if len(chunks) == 0 {
		return false, nil
	}

	msg := buildMessage(f.cfg.GatewayID, f.src.StreamID(), chunks)
	if err := f.adapter.Send(ctx, msg); err != nil {
		f.status.recordFailure(err)
		f.log.Warn("message send failed, will retry", "from_seq", msg.FromSeq, "to_seq", msg.ToSeq, "readings", msg.ReadingCount(), "error", err)
		return false, err
	}
	f.status.recordSuccess()

	if err := f.src.Ack(ctx, msg.ToSeq); err != nil {
		// The server has the data; it will be sent again and de-duplicated.
		f.log.Error("failed to remove acknowledged chunks from the queue", "to_seq", msg.ToSeq, "error", err)
		return false, err
	}
	f.log.Debug("message forwarded", "from_seq", msg.FromSeq, "to_seq", msg.ToSeq, "readings", msg.ReadingCount())
	return true, nil
}
