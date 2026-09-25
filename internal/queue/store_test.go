package queue

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func openTest(t *testing.T, path string, opts Options) *Store {
	t.Helper()
	s, err := Open(path, opts, testLogger())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func val(v float64) *float64 { return &v }

var t0 = time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC)

func reading(i int) Reading {
	return Reading{DeviceID: int64(i%5 + 1), DatapointID: int64(i%14 + 1), Value: val(230 + float64(i%7)/10), Quality: "GOOD", At: t0.Add(time.Duration(i) * 80 * time.Millisecond)}
}

func TestCodecRoundTrip(t *testing.T) {
	in := []Reading{
		{DeviceID: 1, DatapointID: 10, Value: val(231.2), Quality: "GOOD", At: t0},
		{DeviceID: 2, DatapointID: 20, Quality: "TIMEOUT", At: t0.Add(1500 * time.Millisecond)},
		// earlier than the previous record: concurrent pollers append out of order
		{DeviceID: 3, DatapointID: 30, Quality: "SOMETHING_NEW", At: t0.Add(-40 * time.Millisecond)},
		{DeviceID: 4, DatapointID: 40, Value: val(-0.5), Quality: "CRC_ERROR", At: t0.Add(2 * time.Second)},
	}
	blob, err := encodeChunk(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decodeChunk(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d readings, want %d", len(out), len(in))
	}
	for i := range in {
		a, b := in[i], out[i]
		if a.DeviceID != b.DeviceID || a.DatapointID != b.DatapointID || a.Quality != b.Quality || !a.At.Equal(b.At) {
			t.Errorf("reading %d: got %+v, want %+v", i, b, a)
		}
		if (a.Value == nil) != (b.Value == nil) || (a.Value != nil && *a.Value != *b.Value) {
			t.Errorf("reading %d value: got %v, want %v", i, b.Value, a.Value)
		}
	}
}

func TestDecodeRejectsCorruptChunk(t *testing.T) {
	for _, blob := range [][]byte{nil, {9, 1, 2}, {chunkFormatV1, 0xff, 0xff}} {
		if _, err := decodeChunk(blob); err == nil {
			t.Errorf("decodeChunk(%v) succeeded, want error", blob)
		}
	}
}

func TestAppendFlushReadAck(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, filepath.Join(t.TempDir(), "queue.db"), Options{})
	defer s.Close()

	for chunk := 0; chunk < 3; chunk++ {
		for i := 0; i < 25; i++ {
			s.Append(reading(chunk*25 + i))
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	st := s.Stats()
	if st.PendingChunks != 3 || st.PendingReadings != 75 || st.BufferedReadings != 0 {
		t.Fatalf("stats after flush = %+v", st)
	}
	if st.OldestPending == nil || !st.OldestPending.Equal(t0) {
		t.Errorf("OldestPending = %v, want %v", st.OldestPending, t0)
	}

	batch, err := s.ReadBatch(ctx, 30) // stops once >= 30 readings: 2 chunks
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(batch) != 2 || batch[0].Seq >= batch[1].Seq || len(batch[0].Readings) != 25 {
		t.Fatalf("ReadBatch returned %d chunks, seqs %v", len(batch), batch)
	}
	if got := batch[0].Readings[3]; got.DatapointID != reading(3).DatapointID || *got.Value != *reading(3).Value {
		t.Errorf("reading 3 = %+v, want %+v", got, reading(3))
	}

	if err := s.Ack(ctx, batch[1].Seq); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	st = s.Stats()
	if st.PendingChunks != 1 || st.PendingReadings != 25 {
		t.Fatalf("stats after ack = %+v", st)
	}
	if want := reading(50).At; !st.OldestPending.Equal(want) {
		t.Errorf("OldestPending after ack = %v, want %v", st.OldestPending, want)
	}
	rest, _ := s.ReadBatch(ctx, 1000)
	if len(rest) != 1 || rest[0].Seq <= batch[1].Seq {
		t.Fatalf("remaining = %+v", rest)
	}
}

func TestReopenKeepsCountersStreamIDAndNeverReusesSeq(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "queue.db")

	s := openTest(t, path, Options{})
	stream := s.StreamID()
	for i := 0; i < 10; i++ {
		s.Append(reading(i))
	}
	if err := s.Close(); err != nil { // Close flushes
		t.Fatalf("Close: %v", err)
	}

	s = openTest(t, path, Options{})
	if s.StreamID() != stream {
		t.Errorf("stream id changed across reopen: %s -> %s", stream, s.StreamID())
	}
	if st := s.Stats(); st.PendingChunks != 1 || st.PendingReadings != 10 {
		t.Fatalf("stats after reopen = %+v", st)
	}
	b, _ := s.ReadBatch(ctx, 100)
	firstSeq := b[0].Seq
	if err := s.Ack(ctx, firstSeq); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// queue now empty; a new chunk must still get a higher seq
	s = openTest(t, path, Options{})
	defer s.Close()
	s.Append(reading(99))
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	b, _ = s.ReadBatch(ctx, 100)
	if len(b) != 1 || b[0].Seq <= firstSeq {
		t.Fatalf("seq reused: first %d, next %+v", firstSeq, b)
	}
}

func TestAutoVacuumShrinksFileAfterAck(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTest(t, path, Options{})
	defer s.Close()

	var mode int
	if err := s.db.QueryRow(`PRAGMA auto_vacuum`).Scan(&mode); err != nil || mode != 2 {
		t.Fatalf("auto_vacuum = %d (%v), want 2 (INCREMENTAL)", mode, err)
	}

	for c := 0; c < 400; c++ {
		for i := 0; i < 25; i++ {
			s.Append(reading(c*25 + i))
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	s.Maintain(ctx)
	full := s.Stats().DBBytes

	b, _ := s.ReadBatch(ctx, 1<<30)
	if err := s.Ack(ctx, b[len(b)-1].Seq); err != nil {
		t.Fatal(err)
	}
	s.Maintain(ctx)
	if after := s.Stats().DBBytes; after >= full/2 {
		t.Fatalf("db did not shrink after ack: %d -> %d bytes", full, after)
	}
	if fi, err := os.Stat(path + "-wal"); err == nil && fi.Size() != 0 {
		t.Errorf("WAL not truncated: %d bytes", fi.Size())
	}
}

func TestMaintainEvictsOldestOverMaxBytes(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, filepath.Join(t.TempDir(), "queue.db"), Options{MaxBytes: 64 << 10})
	defer s.Close()

	for c := 0; c < 2000; c++ {
		for i := 0; i < 25; i++ {
			s.Append(reading(c*25 + i))
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	before := s.Stats()
	s.Maintain(ctx)
	after := s.Stats()

	if after.DBBytes > 64<<10 {
		t.Errorf("DBBytes = %d, want <= %d", after.DBBytes, 64<<10)
	}
	if after.EvictedReadings == 0 || after.PendingReadings+after.EvictedReadings != before.PendingReadings {
		t.Errorf("evicted %d, pending %d -> %d", after.EvictedReadings, before.PendingReadings, after.PendingReadings)
	}
	// the survivors are the newest chunks
	b, _ := s.ReadBatch(ctx, 1)
	if !after.OldestPending.After(*before.OldestPending) || b[0].Readings[0].At.Before(*after.OldestPending) {
		t.Errorf("oldest pending %v -> %v; first remaining reading %v", before.OldestPending, after.OldestPending, b[0].Readings[0].At)
	}
}

func TestMaintainEvictsWhenDiskFreeSpaceBelowFloor(t *testing.T) {
	ctx := context.Background()
	used := uint64(95)
	s := openTest(t, filepath.Join(t.TempDir(), "queue.db"), Options{
		MinFreePercent: 10,
		DiskUsage:      func() (uint64, uint64, error) { return used, 100, nil }, // 95% used, floor is 90%
	})
	defer s.Close()

	for c := 0; c < 50; c++ {
		s.Append(reading(c))
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	s.Maintain(ctx)
	if st := s.Stats(); st.EvictedReadings == 0 {
		t.Fatalf("nothing evicted with the disk above the free-space floor: %+v", st)
	}

	used = 50
	evicted := s.Stats().EvictedReadings
	s.Maintain(ctx)
	if s.Stats().EvictedReadings != evicted {
		t.Errorf("evicted more with the disk below the floor")
	}
}

func TestFlushFailureKeepsReadingsInMemory(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, filepath.Join(t.TempDir(), "queue.db"), Options{MaxUnflushedChunks: 2})
	defer s.Close()

	if _, err := s.db.Exec(`ALTER TABLE chunk RENAME TO chunk_away`); err != nil {
		t.Fatal(err)
	}
	for c := 0; c < 3; c++ {
		s.Append(reading(c))
		if err := s.Flush(ctx); err == nil {
			t.Fatal("Flush succeeded with the chunk table missing")
		}
	}
	st := s.Stats()
	if st.BufferedReadings != 2 || st.DroppedReadings != 1 {
		t.Fatalf("buffered %d dropped %d, want 2 and 1 (cap of 2 chunks)", st.BufferedReadings, st.DroppedReadings)
	}

	if _, err := s.db.Exec(`ALTER TABLE chunk_away RENAME TO chunk`); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush after recovery: %v", err)
	}
	if st := s.Stats(); st.BufferedReadings != 0 || st.PendingChunks != 2 || st.PendingReadings != 2 {
		t.Fatalf("stats after recovery = %+v", st)
	}
}

func TestReadBatchDiscardsCorruptChunk(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, filepath.Join(t.TempDir(), "queue.db"), Options{})
	defer s.Close()

	for c := 0; c < 2; c++ {
		s.Append(reading(c))
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`UPDATE chunk SET data = x'09' WHERE seq = (SELECT MIN(seq) FROM chunk)`); err != nil {
		t.Fatal(err)
	}
	b, err := s.ReadBatch(ctx, 100)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(b) != 1 {
		t.Fatalf("got %d chunks, want the 1 intact chunk", len(b))
	}
	if st := s.Stats(); st.PendingChunks != 1 || st.DroppedReadings != 1 {
		t.Fatalf("stats = %+v", st)
	}
}
