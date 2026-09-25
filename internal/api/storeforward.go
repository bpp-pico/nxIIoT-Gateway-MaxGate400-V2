package api

import (
	"net/http"
	"time"

	"nxiiot-gateway/internal/queue"
	"nxiiot-gateway/internal/storage"
)

// storeForwardStatusDTO is the Store & Forward panel. Everything queue-side
// comes from queue.Store.Stats(), which is served from memory — V1 had to
// cache a full-table scan here (the 2026-09-10 incident); V2 never scans.
type storeForwardStatusDTO struct {
	Enabled  bool   `json:"enabled"`
	StreamID string `json:"stream_id,omitempty"`
	// PendingReadings are stored in queue.db and not yet acknowledged.
	PendingReadings int64 `json:"pending_readings"`
	PendingChunks   int64 `json:"pending_chunks"`
	// BufferedReadings are in memory, waiting for the next flush.
	BufferedReadings int64      `json:"buffered_readings"`
	OldestPending    *time.Time `json:"oldest_pending,omitempty"`
	NewestPending    *time.Time `json:"newest_pending,omitempty"`
	QueueBytes       int64      `json:"queue_bytes"`
	MaxBytes         int64      `json:"max_bytes"`
	// EvictedReadings: unsent readings deleted because the queue was full,
	// over the life of queue.db. DroppedReadings: lost since start because
	// queue.db could not be written.
	EvictedReadings    int64      `json:"evicted_readings"`
	DroppedReadings    int64      `json:"dropped_readings"`
	WriteRatePerSec    float64    `json:"write_rate_per_sec"`
	AvgBytesPerReading float64    `json:"avg_bytes_per_reading"`
	StorageUsedPercent *float64   `json:"storage_used_percent,omitempty"`
	StorageLevel       string     `json:"storage_level,omitempty"`
	ServerConnected    bool       `json:"server_connected"`
	ServerLastError    string     `json:"server_last_error,omitempty"`
	ServerLastSentAt   *time.Time `json:"server_last_sent_at,omitempty"`
}

func (s *Server) getStoreForwardStatus(w http.ResponseWriter, r *http.Request) {
	dto := storeForwardStatusDTO{MaxBytes: s.cfg.Queue.MaxBytes}

	if s.queue != nil {
		st := s.queue.Stats()
		dto.Enabled = true
		dto.StreamID = st.StreamID
		dto.PendingReadings = st.PendingReadings
		dto.PendingChunks = st.PendingChunks
		dto.BufferedReadings = st.BufferedReadings
		dto.OldestPending = st.OldestPending
		dto.NewestPending = st.NewestPending
		dto.QueueBytes = st.DBBytes
		dto.MaxBytes = st.MaxBytes
		dto.EvictedReadings = st.EvictedReadings
		dto.DroppedReadings = st.DroppedReadings
		dto.WriteRatePerSec = st.WriteRatePerSec
		dto.AvgBytesPerReading = st.AvgBytesPerReading
	}

	if pct, err := storage.DiskUsagePercent(s.cfg.Queue.Path); err == nil {
		dto.StorageUsedPercent = &pct
		dto.StorageLevel = string(queue.ClassifyStorageLevel(pct, 100-s.cfg.Queue.MinFreePercent))
	} else {
		s.log.Warn("failed to read disk usage for store-forward status", "error", err)
	}

	if s.forwarder != nil {
		st := s.forwarder.Status()
		dto.ServerConnected = st.Connected
		dto.ServerLastError = st.LastError
		dto.ServerLastSentAt = st.LastSuccessAt
	}

	writeJSON(w, http.StatusOK, dto)
}

// getStoreForwardStatistics is currently the same view as status; kept as
// a separate endpoint per §21's API list since a dashboard vs. a
// diagnostics page may reasonably want different shapes later (e.g.
// historical throughput), which status alone doesn't cover.
func (s *Server) getStoreForwardStatistics(w http.ResponseWriter, r *http.Request) {
	s.getStoreForwardStatus(w, r)
}

// queueWriteRatePerSec is the live rate readings are queued, 0 when Store
// & Forward is disabled.
func (s *Server) queueWriteRatePerSec() float64 {
	if s.queue == nil {
		return 0
	}
	return s.queue.Stats().WriteRatePerSec
}
