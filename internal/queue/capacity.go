package queue

import (
	"context"
	"math"
	"time"
)

// StorageLevel classifies disk usage for the Store & Forward page:
// NORMAL (<70%), WARNING (70-89%), CRITICAL (90% up to the eviction
// threshold), FULL (at or above it — eviction runs).
type StorageLevel string

const (
	StorageNormal   StorageLevel = "NORMAL"
	StorageWarning  StorageLevel = "WARNING"
	StorageCritical StorageLevel = "CRITICAL"
	StorageFull     StorageLevel = "FULL"
)

// ClassifyStorageLevel maps a disk usage percentage to its band.
// fullPercent is where eviction starts (100 - queue.min_free_percent).
func ClassifyStorageLevel(usedPercent, fullPercent float64) StorageLevel {
	switch {
	case usedPercent >= fullPercent:
		return StorageFull
	case usedPercent >= 90:
		return StorageCritical
	case usedPercent >= 70:
		return StorageWarning
	default:
		return StorageNormal
	}
}

// RunMaintenance enforces the size limits every MaintenanceInterval until
// ctx is cancelled. It runs once immediately.
func (s *Store) RunMaintenance(ctx context.Context) {
	s.Maintain(ctx)
	ticker := time.NewTicker(s.opts.MaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Maintain(ctx)
		}
	}
}

// Maintain runs one maintenance pass:
//  1. return freed pages to the OS (incremental_vacuum) and truncate the
//     WAL, so the file size tracks the data actually pending — V1's WAL
//     reached 870 MB and was never bounded (MEMORY.md 2026-09-11);
//  2. if queue.db is over MaxBytes, or the volume has less than
//     MinFreePercent free, evict the oldest chunks — sent or not — until
//     both limits hold (at most maxEvictPassesPerTick passes of maxEvictChunksPerPass).
func (s *Store) Maintain(ctx context.Context) {
	s.compact(ctx)
	// The chunks-to-evict estimate uses the average chunk size, which is
	// only approximate (page overhead, fragmentation), so repeat a few
	// times rather than wait a whole interval per correction.
	for i := 0; i < maxEvictPassesPerTick; i++ {
		if !s.evictOnce(ctx) {
			return
		}
	}
}

const maxEvictPassesPerTick = 5

// evictOnce evicts one estimated batch if a limit is exceeded and reports
// whether it evicted anything.
func (s *Store) evictOnce(ctx context.Context) bool {
	excess := s.excessBytes()
	if excess <= 0 {
		return false
	}

	s.mu.Lock()
	chunks, size := s.pendingChunks, s.dbBytes
	s.mu.Unlock()
	if chunks <= 0 {
		s.log.Error("queue: storage limit reached but the queue is empty — something else is filling the disk", "excess_bytes", excess)
		return false
	}

	avg := float64(size) / float64(chunks)
	if avg <= 0 {
		avg = 1
	}
	k := min(int64(math.Ceil(float64(excess)/avg))+1, maxEvictChunksPerPass, chunks)

	var cut int64
	err := s.db.QueryRowContext(ctx, `SELECT seq FROM chunk ORDER BY seq LIMIT 1 OFFSET ?`, k-1).Scan(&cut)
	if err != nil {
		s.log.Error("queue: eviction failed to find cut point", "error", err)
		return false
	}
	evChunks, evReadings, err := s.deleteThrough(ctx, cut, "evicted_readings")
	if err != nil {
		s.log.Error("queue: eviction failed", "error", err)
		return false
	}
	s.mu.Lock()
	s.pendingChunks -= evChunks
	s.pendingReadings -= evReadings
	s.evicted += evReadings
	s.mu.Unlock()
	s.refreshBounds(ctx)
	s.compact(ctx)

	s.log.Warn("queue: storage limit reached, oldest unsent readings evicted",
		"evicted_chunks", evChunks, "evicted_readings", evReadings, "excess_bytes", excess)
	return evChunks > 0
}

// excessBytes is how far the queue is over its tightest limit (<= 0: none).
func (s *Store) excessBytes() int64 {
	s.mu.Lock()
	excess := s.dbBytes - s.opts.MaxBytes
	s.mu.Unlock()

	if s.opts.DiskUsage != nil {
		used, total, err := s.opts.DiskUsage()
		if err != nil {
			s.log.Warn("queue: failed to read disk usage", "error", err)
		} else if total > 0 {
			allowed := float64(total) * (100 - s.opts.MinFreePercent) / 100
			excess = max(excess, int64(float64(used)-allowed))
		}
	}
	return excess
}

// compact returns free pages to the OS, truncates the WAL, and refreshes
// the cached file size.
func (s *Store) compact(ctx context.Context) {
	// incremental_vacuum frees pages as the statement is stepped, so it
	// has to be read to the end; a plain Exec stops after the first step
	// and frees almost nothing.
	if rows, err := s.db.QueryContext(ctx, `PRAGMA incremental_vacuum`); err != nil {
		s.log.Warn("queue: incremental_vacuum failed", "error", err)
	} else {
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			s.log.Warn("queue: incremental_vacuum failed", "error", err)
		}
		rows.Close()
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		s.log.Warn("queue: wal checkpoint failed", "error", err)
	}
	s.refreshSize(ctx)
}

func (s *Store) refreshSize(ctx context.Context) {
	var pages, pageSize int64
	if err := s.db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err != nil {
		s.log.Warn("queue: failed to read page_count", "error", err)
		return
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		s.log.Warn("queue: failed to read page_size", "error", err)
		return
	}
	s.mu.Lock()
	s.dbBytes = pages * pageSize
	s.mu.Unlock()
}
