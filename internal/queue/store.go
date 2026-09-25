// Package queue is the Store & Forward buffer: every acquired reading is
// appended here and stays until the downstream server acknowledges it (or
// the buffer runs out of space and evicts the oldest data).
//
// V2 layout (see spec.md "V2 scope"): readings are held in memory for
// FlushInterval, then written as one compressed chunk row per interval to
// a SQLite file of their own (queue.db), separate from gateway.db so a
// full or damaged queue cannot take the configuration down with it. The
// queue is plain FIFO: no per-row status and no priority. A chunk row
// exists only while it is unacknowledged — Ack deletes it — so everything
// in the table is pending by definition. seq (AUTOINCREMENT) is never
// reused, and together with the per-file stream_id it is the idempotency
// key the server de-duplicates on.
package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// Reading is one acquired value as stored in the queue.
type Reading struct {
	DeviceID    int64
	DatapointID int64
	Value       *float64 // nil when Quality is not GOOD
	Quality     string
	At          time.Time
}

// Chunk is one stored row: the readings appended during one flush interval.
type Chunk struct {
	Seq      int64
	Readings []Reading
}

// Options tunes a Store. Zero values take the defaults in Open.
type Options struct {
	// MaxBytes caps queue.db's size; the oldest chunks are evicted above it.
	MaxBytes int64
	// MinFreePercent is the free space kept on the queue's volume; the
	// oldest chunks are evicted when free space drops below it, whatever
	// else is filling the disk. At 0 free space SQLite cannot even run the
	// DELETE that would free space (MEMORY.md 2026-09-11), so this must
	// trigger well before the disk is full.
	MinFreePercent float64
	// FlushInterval is how long readings sit in memory before being
	// written as one chunk — also the most that is lost on power failure.
	FlushInterval time.Duration
	// MaintenanceInterval is how often the size limits are enforced and
	// freed space is returned to the OS.
	MaintenanceInterval time.Duration
	// MaxUnflushedChunks bounds the chunks kept in memory while writes to
	// queue.db keep failing; beyond it the oldest are dropped (counted in
	// Stats.DroppedReadings).
	MaxUnflushedChunks int
	// DiskUsage reports the queue volume's used and total bytes. nil
	// disables the MinFreePercent check (tests).
	DiskUsage func() (used, total uint64, err error)
}

const (
	defaultMaxBytes            = 1 << 30 // 1 GiB (user decision 2026-09-25)
	defaultMinFreePercent      = 10
	defaultFlushInterval       = 2 * time.Second
	defaultMaintenanceInterval = 30 * time.Second
	defaultMaxUnflushedChunks  = 150 // 5 minutes at the default flush interval

	// maxEvictChunksPerPass bounds one eviction DELETE so it never holds
	// the write lock for long (the 2026-09-10 50,000-row DELETE did).
	maxEvictChunksPerPass = 20000
	// readBatchScanLimit caps the rows one ReadBatch query looks at.
	readBatchScanLimit = 500
)

// Stats is a point-in-time view of the queue, served from memory — no
// table scan, unlike V1's data_queue Stats().
type Stats struct {
	StreamID         string
	PendingChunks    int64 // stored in queue.db, not yet acknowledged
	PendingReadings  int64
	BufferedReadings int64 // in memory, not yet written to queue.db
	OldestPending    *time.Time
	NewestPending    *time.Time
	DBBytes          int64
	MaxBytes         int64
	// EvictedReadings counts unsent readings deleted because the queue hit
	// a size limit, over the life of queue.db.
	EvictedReadings int64
	// DroppedReadings counts readings lost since process start because
	// queue.db could not be written (or a stored chunk could not be read).
	DroppedReadings int64
	WriteRatePerSec float64
	// AvgBytesPerReading is the compressed chunk size per reading over
	// every chunk written since process start (SQLite page overhead not
	// included). 0 until the first chunk is written.
	AvgBytesPerReading float64
}

type pendingChunk struct {
	firstTs, lastTs int64
	n               int64
	data            []byte
}

type rateSample struct {
	at    time.Time
	total int64
}

// Store is the on-disk queue. Append is safe to call from any goroutine;
// RunFlusher and RunMaintenance each run in their own.
type Store struct {
	db       *sql.DB
	opts     Options
	log      *slog.Logger
	streamID string

	flushMu  sync.Mutex // serializes Flush (ticker and Close)
	appended atomic.Int64

	mu              sync.Mutex
	buf             []Reading
	unflushed       []pendingChunk
	pendingChunks   int64
	pendingReadings int64
	oldestTs        int64 // unix ms of the oldest stored chunk, 0 when none
	newestTs        int64
	dbBytes         int64
	evicted         int64
	dropped         int64
	rates           []rateSample
	writtenBytes    int64 // chunk blob bytes written since start
	writtenReadings int64
}

// Open opens (creating if needed) the queue database at path.
func Open(path string, opts Options, log *slog.Logger) (*Store, error) {
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = defaultMaxBytes
	}
	if opts.MinFreePercent <= 0 {
		opts.MinFreePercent = defaultMinFreePercent
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	if opts.MaintenanceInterval <= 0 {
		opts.MaintenanceInterval = defaultMaintenanceInterval
	}
	if opts.MaxUnflushedChunks <= 0 {
		opts.MaxUnflushedChunks = defaultMaxUnflushedChunks
	}

	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create queue directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("open queue db: %w", err)
	}
	// One connection: every statement here is short, and a single
	// connection rules out writer-writer contention and stale-snapshot
	// errors entirely (MEMORY.md 2026-09-02).
	db.SetMaxOpenConns(1)

	s := &Store{db: db, opts: opts, log: log}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	// auto_vacuum only takes effect if set before the first table is
	// created; on an existing file this is a no-op, which is fine because
	// this code created it. INCREMENTAL lets RunMaintenance hand freed
	// pages back to the OS, so the file shrinks as the backlog drains
	// (V1's gateway.db never did — MEMORY.md 2026-09-11).
	for _, stmt := range []string{
		`PRAGMA auto_vacuum = INCREMENTAL`,
		`PRAGMA journal_mode = WAL`,
		`CREATE TABLE IF NOT EXISTS chunk (
			seq      INTEGER PRIMARY KEY AUTOINCREMENT,
			first_ts INTEGER NOT NULL,
			last_ts  INTEGER NOT NULL,
			n        INTEGER NOT NULL,
			data     BLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value NOT NULL)`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("init queue db (%s): %w", stmt, err)
		}
	}

	// stream_id identifies this queue.db file. seq restarts at 1 if the
	// file is ever deleted, so the server must de-duplicate on
	// (gateway_id, stream_id, seq), never on seq alone.
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'stream_id'`).Scan(&s.streamID); errors.Is(err, sql.ErrNoRows) {
		s.streamID = uuid.NewString()
		if _, err := s.db.Exec(`INSERT INTO meta (key, value) VALUES ('stream_id', ?)`, s.streamID); err != nil {
			return fmt.Errorf("store stream_id: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("read stream_id: %w", err)
	}

	// Counters live in meta and are updated in the same transaction as
	// the rows they count, so startup does not have to scan the table.
	s.pendingChunks = s.metaInt("pending_chunks")
	s.pendingReadings = s.metaInt("pending_readings")
	s.evicted = s.metaInt("evicted_readings")
	s.refreshBounds(context.Background())
	s.refreshSize(context.Background())
	return nil
}

func (s *Store) metaInt(key string) int64 {
	var v int64
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v); err != nil && !errors.Is(err, sql.ErrNoRows) {
		s.log.Error("queue: failed to read counter", "key", key, "error", err)
	}
	return v
}

func addMeta(ctx context.Context, tx *sql.Tx, key string, delta int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO meta (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = value + excluded.value`, key, delta)
	return err
}

// StreamID identifies this queue file; see init.
func (s *Store) StreamID() string { return s.streamID }

// Append buffers one reading; it is written to disk at the next flush.
func (s *Store) Append(r Reading) {
	s.mu.Lock()
	s.buf = append(s.buf, r)
	s.mu.Unlock()
	s.appended.Add(1)
}

// Flush encodes the buffered readings into a chunk and writes every chunk
// still waiting in memory in one transaction. On a write error the chunks
// stay in memory (up to MaxUnflushedChunks) and are retried next time —
// V1 dropped a reading the moment its insert failed.
func (s *Store) Flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	if len(s.buf) > 0 {
		readings := s.buf
		s.buf = nil
		if data, err := encodeChunk(readings); err != nil {
			s.dropped += int64(len(readings))
			s.log.Error("queue: failed to encode chunk, readings dropped", "readings", len(readings), "error", err)
		} else {
			first, last := readings[0].At.UnixMilli(), readings[0].At.UnixMilli()
			for _, r := range readings[1:] {
				ts := r.At.UnixMilli()
				first, last = min(first, ts), max(last, ts)
			}
			s.unflushed = append(s.unflushed, pendingChunk{firstTs: first, lastTs: last, n: int64(len(readings)), data: data})
		}
	}
	batch := s.unflushed
	s.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}

	err := s.insertChunks(ctx, batch)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if over := len(s.unflushed) - s.opts.MaxUnflushedChunks; over > 0 {
			for _, c := range s.unflushed[:over] {
				s.dropped += c.n
			}
			s.log.Error("queue: in-memory backlog full while queue.db writes fail, oldest readings dropped", "chunks", over)
			s.unflushed = append([]pendingChunk(nil), s.unflushed[over:]...)
		}
		return fmt.Errorf("write chunks: %w", err)
	}
	// Only Flush touches unflushed, under flushMu, so it still starts with
	// exactly the chunks just written.
	s.unflushed = append([]pendingChunk(nil), s.unflushed[len(batch):]...)
	for _, c := range batch {
		s.pendingChunks++
		s.pendingReadings += c.n
		s.writtenBytes += int64(len(c.data))
		s.writtenReadings += c.n
		if s.oldestTs == 0 {
			s.oldestTs = c.firstTs
		}
		s.newestTs = max(s.newestTs, c.lastTs)
	}
	return nil
}

func (s *Store) insertChunks(ctx context.Context, batch []pendingChunk) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var readings int64
	for _, c := range batch {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO chunk (first_ts, last_ts, n, data) VALUES (?, ?, ?, ?)`,
			c.firstTs, c.lastTs, c.n, c.data); err != nil {
			return err
		}
		readings += c.n
	}
	if err := addMeta(ctx, tx, "pending_chunks", int64(len(batch))); err != nil {
		return err
	}
	if err := addMeta(ctx, tx, "pending_readings", readings); err != nil {
		return err
	}
	return tx.Commit()
}

// RunFlusher flushes every FlushInterval until ctx is cancelled. The last
// readings are written by Close, which the owner calls once acquisition
// has stopped appending.
func (s *Store) RunFlusher(ctx context.Context) {
	ticker := time.NewTicker(s.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Flush(ctx); err != nil {
				s.log.Error("queue: flush failed, will retry", "error", err)
			}
			s.sampleRate()
		}
	}
}

// rateWindow is how many flush ticks the write rate is averaged over.
const rateWindow = 16

func (s *Store) sampleRate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rates = append(s.rates, rateSample{at: time.Now(), total: s.appended.Load()})
	if len(s.rates) > rateWindow {
		s.rates = s.rates[len(s.rates)-rateWindow:]
	}
}

// ReadBatch returns the oldest pending chunks, stopping once at least
// maxReadings readings are collected (always at least one chunk when any
// is pending). A chunk that cannot be decoded is deleted and counted as
// dropped rather than blocking the queue forever.
func (s *Store) ReadBatch(ctx context.Context, maxReadings int) ([]Chunk, error) {
	type row struct {
		seq  int64
		n    int64
		data []byte
	}
	rows, err := s.db.QueryContext(ctx, `SELECT seq, n, data FROM chunk ORDER BY seq LIMIT ?`, readBatchScanLimit)
	if err != nil {
		return nil, err
	}
	var raw []row
	var total int64
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.seq, &r.n, &r.data); err != nil {
			rows.Close()
			return nil, err
		}
		raw = append(raw, r)
		total += r.n
		if total >= int64(maxReadings) {
			break
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	chunks := make([]Chunk, 0, len(raw))
	for _, r := range raw {
		readings, err := decodeChunk(r.data)
		if err != nil {
			s.log.Error("queue: corrupt chunk discarded", "seq", r.seq, "readings", r.n, "error", err)
			if derr := s.discard(ctx, r.seq, r.n); derr != nil {
				return nil, derr
			}
			continue
		}
		chunks = append(chunks, Chunk{Seq: r.seq, Readings: readings})
	}
	return chunks, nil
}

func (s *Store) discard(ctx context.Context, seq, n int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM chunk WHERE seq = ?`, seq)
	if err != nil {
		return err
	}
	if k, _ := res.RowsAffected(); k == 0 {
		return nil
	}
	if err := addMeta(ctx, tx, "pending_chunks", -1); err != nil {
		return err
	}
	if err := addMeta(ctx, tx, "pending_readings", -n); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.mu.Lock()
	s.pendingChunks--
	s.pendingReadings -= n
	s.dropped += n
	s.mu.Unlock()
	s.refreshBounds(ctx)
	return nil
}

// Ack deletes every chunk with seq <= throughSeq: the server has them.
func (s *Store) Ack(ctx context.Context, throughSeq int64) error {
	chunks, readings, err := s.deleteThrough(ctx, throughSeq, "")
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.pendingChunks -= chunks
	s.pendingReadings -= readings
	s.mu.Unlock()
	s.refreshBounds(ctx)
	return nil
}

// deleteThrough removes chunks with seq <= throughSeq in one transaction,
// keeps the meta counters in step, and adds the removed readings to
// countKey when it is set.
func (s *Store) deleteThrough(ctx context.Context, throughSeq int64, countKey string) (chunks, readings int64, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(n), 0) FROM chunk WHERE seq <= ?`, throughSeq,
	).Scan(&chunks, &readings); err != nil {
		return 0, 0, err
	}
	if chunks == 0 {
		return 0, 0, nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM chunk WHERE seq <= ?`, throughSeq); err != nil {
		return 0, 0, err
	}
	if err := addMeta(ctx, tx, "pending_chunks", -chunks); err != nil {
		return 0, 0, err
	}
	if err := addMeta(ctx, tx, "pending_readings", -readings); err != nil {
		return 0, 0, err
	}
	if countKey != "" {
		if err := addMeta(ctx, tx, countKey, readings); err != nil {
			return 0, 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return chunks, readings, nil
}

// refreshBounds re-reads the oldest/newest pending timestamps. Both are
// lookups at either end of the primary key, not scans.
func (s *Store) refreshBounds(ctx context.Context) {
	var oldest, newest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT first_ts FROM chunk ORDER BY seq ASC LIMIT 1`).Scan(&oldest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		s.log.Warn("queue: failed to read oldest chunk", "error", err)
		return
	}
	if err := s.db.QueryRowContext(ctx, `SELECT last_ts FROM chunk ORDER BY seq DESC LIMIT 1`).Scan(&newest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		s.log.Warn("queue: failed to read newest chunk", "error", err)
		return
	}
	s.mu.Lock()
	s.oldestTs, s.newestTs = oldest.Int64, newest.Int64
	s.mu.Unlock()
}

// Stats returns the current counters without touching the database.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := Stats{
		StreamID:        s.streamID,
		PendingChunks:   s.pendingChunks,
		PendingReadings: s.pendingReadings,
		DBBytes:         s.dbBytes,
		MaxBytes:        s.opts.MaxBytes,
		EvictedReadings: s.evicted,
		DroppedReadings: s.dropped,
	}
	st.BufferedReadings = int64(len(s.buf))
	for _, c := range s.unflushed {
		st.BufferedReadings += c.n
	}

	oldest, newest := s.oldestTs, s.newestTs
	if oldest == 0 && len(s.unflushed) > 0 {
		oldest = s.unflushed[0].firstTs
	}
	if oldest == 0 && len(s.buf) > 0 {
		oldest = s.buf[0].At.UnixMilli()
	}
	if len(s.buf) > 0 {
		newest = max(newest, s.buf[len(s.buf)-1].At.UnixMilli())
	}
	if oldest != 0 {
		t := time.UnixMilli(oldest).UTC()
		st.OldestPending = &t
	}
	if newest != 0 {
		t := time.UnixMilli(newest).UTC()
		st.NewestPending = &t
	}

	if s.writtenReadings > 0 {
		st.AvgBytesPerReading = float64(s.writtenBytes) / float64(s.writtenReadings)
	}
	if n := len(s.rates); n >= 2 {
		first, last := s.rates[0], s.rates[n-1]
		if secs := last.at.Sub(first.at).Seconds(); secs > 0 {
			st.WriteRatePerSec = float64(last.total-first.total) / secs
		}
	}
	return st
}

// Close flushes what is buffered and closes the database.
func (s *Store) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	flushErr := s.Flush(ctx)
	return errors.Join(flushErr, s.db.Close())
}
