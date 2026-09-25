package main

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"nxiiot-gateway/internal/forwarder"
)

// maxReceived bounds how many recent readings /received keeps in memory.
const maxReceived = 10000

// reading is one stored value, flattened for /received.
type reading struct {
	GatewayID string `json:"gateway_id"`
	StreamID  string `json:"stream_id"`
	Seq       int64  `json:"seq"`
	forwarder.WireReading
}

type chunkKey struct {
	gatewayID, streamID string
	seq                 int64
}

type streamKey struct{ gatewayID, streamID string }

type dpKey struct {
	gatewayID             string
	deviceID, datapointID int64
}

// gap is a seq range missing from a stream: the gateway evicted it.
type gap struct {
	GatewayID string    `json:"gateway_id"`
	StreamID  string    `json:"stream_id"`
	FromSeq   int64     `json:"from_seq"`
	ToSeq     int64     `json:"to_seq"`
	At        time.Time `json:"at"`
}

// gatewayStats is what the status page shows per gateway. Counters live in
// memory and restart from zero when server-sim restarts.
type gatewayStats struct {
	ID              string           `json:"id"`
	LastSeen        time.Time        `json:"last_seen"`
	Messages        int64            `json:"messages"`
	Readings        int64            `json:"readings"`
	DuplicateChunks int64            `json:"duplicate_chunks"`
	Gaps            int64            `json:"gaps"`
	MissingChunks   int64            `json:"missing_chunks"`
	Rejected        int64            `json:"rejected"`
	LastError       string           `json:"last_error,omitempty"`
	Streams         map[string]int64 `json:"streams"` // stream_id → highest seq
}

// datapointSeen is a datapoint discovered from incoming data.
type datapointSeen struct {
	GatewayID   string   `json:"gateway_id"`
	DeviceID    int64    `json:"device_id"`
	DatapointID int64    `json:"datapoint_id"`
	LastTS      string   `json:"last_ts"`
	LastValue   *float64 `json:"last_value"`
	LastQuality string   `json:"last_quality"`
}

// chunkWriter persists new chunks; nil means memory only.
type chunkWriter interface {
	writeChunks(ctx context.Context, gatewayID, streamID string, chunks []forwarder.WireChunk) error
	writeGap(ctx context.Context, g gap) error
}

type store struct {
	mu         sync.Mutex
	out        chunkWriter
	seen       map[chunkKey]bool
	lastSeq    map[streamKey]int64
	received   []reading
	gateways   map[string]*gatewayStats
	datapoints map[dpKey]*datapointSeen
	gaps       []gap // most recent first, capped
}

func newStore(out chunkWriter) *store {
	return &store{
		out:        out,
		seen:       make(map[chunkKey]bool),
		lastSeq:    make(map[streamKey]int64),
		gateways:   make(map[string]*gatewayStats),
		datapoints: make(map[dpKey]*datapointSeen),
	}
}

func (s *store) gw(id string) *gatewayStats {
	g := s.gateways[id]
	if g == nil {
		g = &gatewayStats{ID: id, Streams: map[string]int64{}}
		s.gateways[id] = g
	}
	return g
}

// noteMessage records that a message arrived from gatewayID.
func (s *store) noteMessage(gatewayID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gw(gatewayID)
	g.Messages++
	g.LastSeen = time.Now().UTC()
}

// noteRejected records a message that was not stored.
func (s *store) noteRejected(gatewayID, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gw(gatewayID)
	g.Rejected++
	g.LastError = reason
}

// ingest stores every chunk of m not seen before and reports how many
// readings were new and how many chunks were duplicates. A non-nil error
// means nothing was stored and the message must not be acked.
func (s *store) ingest(ctx context.Context, m forwarder.Message) (accepted, duplicateChunks int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var fresh []forwarder.WireChunk
	for _, c := range m.Chunks {
		if s.seen[chunkKey{m.GatewayID, m.StreamID, c.Seq}] {
			duplicateChunks++
			continue
		}
		fresh = append(fresh, c)
	}
	g := s.gw(m.GatewayID)
	g.DuplicateChunks += int64(duplicateChunks)
	if s.out != nil && len(fresh) > 0 {
		if err := s.out.writeChunks(ctx, m.GatewayID, m.StreamID, fresh); err != nil {
			g.Rejected++
			g.LastError = err.Error()
			return 0, duplicateChunks, err
		}
	}

	sk := streamKey{m.GatewayID, m.StreamID}
	for _, c := range fresh {
		s.seen[chunkKey{m.GatewayID, m.StreamID, c.Seq}] = true
		if last, ok := s.lastSeq[sk]; ok && c.Seq > last+1 {
			gp := gap{GatewayID: m.GatewayID, StreamID: m.StreamID, FromSeq: last + 1, ToSeq: c.Seq - 1, At: time.Now().UTC()}
			log.Printf("gap: gateway=%s stream=%s seq %d-%d missing (evicted on the gateway)", gp.GatewayID, gp.StreamID, gp.FromSeq, gp.ToSeq)
			g.Gaps++
			g.MissingChunks += gp.ToSeq - gp.FromSeq + 1
			s.gaps = append([]gap{gp}, s.gaps...)
			if len(s.gaps) > 100 {
				s.gaps = s.gaps[:100]
			}
			if s.out != nil {
				if err := s.out.writeGap(ctx, gp); err != nil {
					log.Printf("gap: could not write to InfluxDB: %v", err)
				}
			}
		}
		if c.Seq > s.lastSeq[sk] {
			s.lastSeq[sk] = c.Seq
			g.Streams[m.StreamID] = c.Seq
		}
		for _, r := range c.Readings {
			s.received = append(s.received, reading{GatewayID: m.GatewayID, StreamID: m.StreamID, Seq: c.Seq, WireReading: r})
			k := dpKey{m.GatewayID, r.DeviceID, r.DatapointID}
			d := s.datapoints[k]
			if d == nil {
				d = &datapointSeen{GatewayID: k.gatewayID, DeviceID: k.deviceID, DatapointID: k.datapointID}
				s.datapoints[k] = d
			}
			if r.Timestamp >= d.LastTS { // RFC 3339 UTC ms strings sort by time
				d.LastTS, d.LastValue, d.LastQuality = r.Timestamp, r.Value, r.Quality
			}
			accepted++
		}
	}
	g.Readings += int64(accepted)
	g.LastError = ""
	if n := len(s.received); n > maxReceived {
		s.received = append(s.received[:0], s.received[n-maxReceived:]...)
	}
	return accepted, duplicateChunks, nil
}

func (s *store) all() []reading {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]reading, len(s.received))
	copy(out, s.received)
	return out
}

// snapshot copies the status for the status API, sorted for display.
func (s *store) snapshot() (gws []gatewayStats, dps []datapointSeen, gaps []gap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.gateways {
		c := *g
		c.Streams = make(map[string]int64, len(g.Streams))
		for k, v := range g.Streams {
			c.Streams[k] = v
		}
		gws = append(gws, c)
	}
	sort.Slice(gws, func(i, j int) bool { return gws[i].ID < gws[j].ID })
	for _, d := range s.datapoints {
		dps = append(dps, *d)
	}
	sort.Slice(dps, func(i, j int) bool {
		a, b := dps[i], dps[j]
		if a.GatewayID != b.GatewayID {
			return a.GatewayID < b.GatewayID
		}
		if a.DeviceID != b.DeviceID {
			return a.DeviceID < b.DeviceID
		}
		return a.DatapointID < b.DatapointID
	})
	gaps = append(gaps, s.gaps...)
	return gws, dps, gaps
}
