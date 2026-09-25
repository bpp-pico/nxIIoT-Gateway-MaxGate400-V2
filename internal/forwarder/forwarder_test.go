package forwarder_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"nxiiot-gateway/internal/forwarder"
	"nxiiot-gateway/internal/queue"
)

func openTestQueue(t *testing.T) *queue.Store {
	t.Helper()
	s, err := queue.Open(filepath.Join(t.TempDir(), "queue.db"), queue.Options{}, newTestLogger(t))
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// fill writes chunks x perChunk readings to the queue, one flush per chunk.
func fill(t *testing.T, s *queue.Store, chunks, perChunk int) {
	t.Helper()
	v := 1.5
	for c := 0; c < chunks; c++ {
		for i := 0; i < perChunk; i++ {
			s.Append(queue.Reading{DeviceID: 1, DatapointID: int64(i + 1), Value: &v, Quality: "GOOD", At: time.Now()})
		}
		if err := s.Flush(context.Background()); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// fakeAdapter lets tests script success/failure per call and records every
// message it was asked to send.
type fakeAdapter struct {
	mu   sync.Mutex
	fail func(callNum int) error // nil = always succeed
	msgs []forwarder.Message
}

func (a *fakeAdapter) Send(ctx context.Context, msg forwarder.Message) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.msgs = append(a.msgs, msg)
	if a.fail != nil {
		return a.fail(len(a.msgs))
	}
	return nil
}

func (a *fakeAdapter) sent() []forwarder.Message {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]forwarder.Message(nil), a.msgs...)
}

func newTestLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, nil))
}

func TestForwarderSendsPendingChunksAndAcksThem(t *testing.T) {
	q := openTestQueue(t)
	fill(t, q, 3, 5)

	adapter := &fakeAdapter{}
	fwd := forwarder.New(q, adapter, forwarder.Config{GatewayID: "GW001", MaxReadingsPerMessage: 1000, PollInterval: 10 * time.Millisecond}, newTestLogger(t))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	fwd.Run(ctx)

	msgs := adapter.sent()
	if len(msgs) != 1 {
		t.Fatalf("sent %d messages, want 1 carrying all 3 chunks", len(msgs))
	}
	m := msgs[0]
	if m.GatewayID != "GW001" || m.StreamID != q.StreamID() || len(m.Chunks) != 3 || m.ReadingCount() != 15 {
		t.Fatalf("message = %+v", m)
	}
	if m.FromSeq != m.Chunks[0].Seq || m.ToSeq != m.Chunks[2].Seq || m.FromSeq >= m.ToSeq {
		t.Errorf("seq range %d-%d does not match chunks", m.FromSeq, m.ToSeq)
	}
	if st := q.Stats(); st.PendingChunks != 0 {
		t.Errorf("PendingChunks = %d after ack, want 0", st.PendingChunks)
	}
	if !fwd.Status().Connected {
		t.Error("expected Connected after a successful send")
	}
}

func TestForwarderSplitsBacklogIntoMessagesInSeqOrder(t *testing.T) {
	q := openTestQueue(t)
	fill(t, q, 10, 4) // 40 readings, 4 per chunk

	adapter := &fakeAdapter{}
	fwd := forwarder.New(q, adapter, forwarder.Config{GatewayID: "GW001", MaxReadingsPerMessage: 8, PollInterval: 10 * time.Millisecond}, newTestLogger(t))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	fwd.Run(ctx)

	msgs := adapter.sent()
	if len(msgs) != 5 {
		t.Fatalf("sent %d messages, want 5 of 2 chunks each", len(msgs))
	}
	for i := 1; i < len(msgs); i++ {
		if msgs[i].FromSeq <= msgs[i-1].ToSeq {
			t.Errorf("message %d starts at %d, not after %d", i, msgs[i].FromSeq, msgs[i-1].ToSeq)
		}
	}
	if st := q.Stats(); st.PendingReadings != 0 {
		t.Errorf("PendingReadings = %d, want 0", st.PendingReadings)
	}
}

func TestForwarderResendsSameDataAfterFailure(t *testing.T) {
	q := openTestQueue(t)
	fill(t, q, 1, 3)

	adapter := &fakeAdapter{fail: func(n int) error {
		if n == 1 {
			return errors.New("simulated server down")
		}
		return nil
	}}
	fwd := forwarder.New(q, adapter, forwarder.Config{GatewayID: "GW001", PollInterval: 10 * time.Millisecond}, newTestLogger(t))
	// first backoff is 1s
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	fwd.Run(ctx)

	msgs := adapter.sent()
	if len(msgs) != 2 {
		t.Fatalf("sent %d messages, want 2 (fail, then retry after backoff)", len(msgs))
	}
	if msgs[0].FromSeq != msgs[1].FromSeq || msgs[0].ToSeq != msgs[1].ToSeq {
		t.Errorf("retry sent %d-%d, want the same %d-%d", msgs[1].FromSeq, msgs[1].ToSeq, msgs[0].FromSeq, msgs[0].ToSeq)
	}
	if st := q.Stats(); st.PendingChunks != 0 {
		t.Errorf("PendingChunks = %d, want 0 after the retry succeeded", st.PendingChunks)
	}
	if !fwd.Status().Connected {
		t.Error("expected Connected after the retry succeeded")
	}
}

func TestForwarderKeepsDataWhenServerNeverAcks(t *testing.T) {
	q := openTestQueue(t)
	fill(t, q, 2, 3)

	adapter := &fakeAdapter{fail: func(int) error { return errors.New("down") }}
	fwd := forwarder.New(q, adapter, forwarder.Config{GatewayID: "GW001", PollInterval: 10 * time.Millisecond}, newTestLogger(t))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	fwd.Run(ctx)

	if st := q.Stats(); st.PendingChunks != 2 || st.PendingReadings != 6 {
		t.Errorf("stats = %+v, want all data still pending", st)
	}
	if fwd.Status().Connected {
		t.Error("expected not Connected")
	}
}

func TestForwarderDoesNotCallAdapterWhenQueueIsEmpty(t *testing.T) {
	q := openTestQueue(t)
	adapter := &fakeAdapter{}
	fwd := forwarder.New(q, adapter, forwarder.Config{GatewayID: "GW001", PollInterval: 10 * time.Millisecond}, newTestLogger(t))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	fwd.Run(ctx)

	if n := len(adapter.sent()); n != 0 {
		t.Errorf("expected 0 sends with an empty queue, got %d", n)
	}
}
