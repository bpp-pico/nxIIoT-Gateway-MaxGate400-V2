package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"nxiiot-gateway/internal/forwarder"
)

//go:embed ui/index.html
var indexHTML []byte

type server struct {
	settings  *settingsStore
	store     *store
	influx    *influxWriter // nil: InfluxDB disabled
	mqtt      *mqttConsumer
	startedAt time.Time
}

// result is what process decided for one message.
type result struct {
	reply    bool   // false: send no ack at all
	err      string // non-empty: ack with this error (nothing was stored)
	status   int    // HTTP status for the HTTP transport
	accepted int
	dups     int
}

// process applies the gateway registry and the simulated fault, then
// stores the message. Both transports go through here.
func (sv *server) process(ctx context.Context, m forwarder.Message) result {
	set := sv.settings.get()
	sv.store.noteMessage(m.GatewayID)

	if set.Fault.Mode == faultIgnore {
		sv.store.noteRejected(m.GatewayID, "simulated: message ignored")
		return result{}
	}
	if _, ok := set.gatewayName(m.GatewayID); !ok && set.RejectUnknownGateways {
		sv.store.noteRejected(m.GatewayID, "gateway not registered")
		return result{reply: true, err: "gateway " + m.GatewayID + " is not registered on this server", status: http.StatusForbidden}
	}
	if set.Fault.Mode == faultErrorAck {
		sv.store.noteRejected(m.GatewayID, "simulated: error ack")
		return result{reply: true, err: "simulated server error", status: http.StatusServiceUnavailable}
	}

	accepted, dups, err := sv.store.ingest(ctx, m)
	if err != nil {
		return result{reply: true, err: err.Error(), status: http.StatusServiceUnavailable, dups: dups}
	}
	if d := set.Fault.AckDelayMs; d > 0 {
		select {
		case <-time.After(time.Duration(d) * time.Millisecond):
		case <-ctx.Done():
		}
	}
	if set.Fault.Mode == faultDropAck {
		return result{accepted: accepted, dups: dups}
	}
	return result{reply: true, status: http.StatusOK, accepted: accepted, dups: dups}
}

func logResult(transport string, m forwarder.Message, r result) {
	switch {
	case r.err != "":
		log.Printf("%s ingest: gateway=%s seq=%d-%d rejected: %s", transport, m.GatewayID, m.FromSeq, m.ToSeq, r.err)
	case !r.reply:
		log.Printf("%s ingest: gateway=%s seq=%d-%d readings=%d accepted=%d duplicate_chunks=%d (no ack: fault mode)", transport, m.GatewayID, m.FromSeq, m.ToSeq, m.ReadingCount(), r.accepted, r.dups)
	default:
		log.Printf("%s ingest: gateway=%s seq=%d-%d readings=%d accepted=%d duplicate_chunks=%d", transport, m.GatewayID, m.FromSeq, m.ToSeq, m.ReadingCount(), r.accepted, r.dups)
	}
}

// handleMQTT is the mqttConsumer callback: it returns the ack to publish.
func (sv *server) handleMQTT(payload []byte) []byte {
	var m forwarder.Message
	if err := json.Unmarshal(payload, &m); err != nil {
		log.Printf("mqtt: invalid message payload: %v", err)
		return nil
	}
	r := sv.process(context.Background(), m)
	logResult("mqtt", m, r)
	if !r.reply {
		return nil
	}
	// Ack only after the data is stored; echo to_seq (and stream_id).
	b, _ := json.Marshal(forwarder.Ack{StreamID: m.StreamID, ToSeq: m.ToSeq, Error: r.err})
	return b
}

func (sv *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest", sv.handleIngest)
	mux.HandleFunc("GET /received", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, sv.store.all())
	})
	mux.HandleFunc("GET /api/status", sv.handleStatus)
	mux.HandleFunc("GET /api/settings", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, sv.settings.get())
	})
	mux.HandleFunc("PUT /api/settings", sv.handlePutSettings)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	return mux
}

func (sv *server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var m forwarder.Message
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	res := sv.process(r.Context(), m)
	logResult("http", m, res)
	if !res.reply {
		// No response at all, like a lost ack: drop the connection.
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
				return
			}
		}
		http.Error(w, "no ack (fault mode)", http.StatusGatewayTimeout)
		return
	}
	if res.err != "" {
		http.Error(w, res.err, res.status)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"accepted": res.accepted, "duplicates": res.dups})
}

type gatewayStatus struct {
	gatewayStats
	Name       string `json:"name"`
	Registered bool   `json:"registered"`
}

type datapointStatus struct {
	datapointSeen
	Name string `json:"name"`
	Unit string `json:"unit"`
}

func (sv *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	set := sv.settings.get()
	gws, dps, gaps := sv.store.snapshot()

	// Registered gateways that have not sent anything yet are listed too.
	gwOut := []gatewayStatus{}
	have := map[string]bool{}
	for _, g := range gws {
		name, reg := set.gatewayName(g.ID)
		gwOut = append(gwOut, gatewayStatus{gatewayStats: g, Name: name, Registered: reg})
		have[g.ID] = true
	}
	for _, g := range set.Gateways {
		if !have[g.ID] {
			gwOut = append(gwOut, gatewayStatus{gatewayStats: gatewayStats{ID: g.ID, Streams: map[string]int64{}}, Name: g.Name, Registered: true})
		}
	}

	// Named datapoints that have not been seen since start are listed too.
	names := map[dpKey]DatapointName{}
	for _, d := range set.Datapoints {
		names[dpKey{d.GatewayID, d.DeviceID, d.DatapointID}] = d
	}
	dpOut := []datapointStatus{}
	for _, d := range dps {
		k := dpKey{d.GatewayID, d.DeviceID, d.DatapointID}
		n := names[k]
		delete(names, k)
		dpOut = append(dpOut, datapointStatus{datapointSeen: d, Name: n.Name, Unit: n.Unit})
	}
	for _, n := range set.Datapoints {
		if _, left := names[dpKey{n.GatewayID, n.DeviceID, n.DatapointID}]; left {
			dpOut = append(dpOut, datapointStatus{datapointSeen: datapointSeen{GatewayID: n.GatewayID, DeviceID: n.DeviceID, DatapointID: n.DatapointID}, Name: n.Name, Unit: n.Unit})
		}
	}

	broker, topic, connected := sv.mqtt.status()
	influx := map[string]any{"enabled": sv.influx != nil}
	if sv.influx != nil {
		influx["url"] = sv.influx.baseURL
		influx["org"] = sv.influx.org
		influx["last_error"] = sv.influx.lastError()
	}
	if gaps == nil {
		gaps = []gap{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"started_at": sv.startedAt,
		"fault":      set.Fault,
		"mqtt":       map[string]any{"broker_url": broker, "data_topic": topic, "connected": connected},
		"influx":     influx,
		"gateways":   gwOut,
		"datapoints": dpOut,
		"gaps":       gaps,
	})
}

func (sv *server) handlePutSettings(w http.ResponseWriter, r *http.Request) {
	var next Settings
	if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	next.normalize()
	if err := next.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	prev := sv.settings.get()
	// Make sure the bucket exists before any write is pointed at it.
	if sv.influx != nil && next.Influx != prev.Influx {
		if err := sv.influx.ensureBucket(r.Context(), next.Influx.Bucket, next.Influx.RetentionDays); err != nil {
			http.Error(w, "InfluxDB bucket: "+err.Error(), http.StatusBadGateway)
			return
		}
	}
	saved, err := sv.settings.set(next)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sv.mqtt.apply(saved.MQTT.BrokerURL, saved.MQTT.DataTopic)
	if sv.influx != nil {
		if err := sv.influx.writeMeta(r.Context(), saved.Datapoints); err != nil {
			log.Printf("settings: could not write datapoint names to InfluxDB: %v", err)
		}
	}
	log.Printf("settings saved: fault=%s delay=%dms reject_unknown=%v gateways=%d named_datapoints=%d mqtt=%s %s bucket=%s retention=%dd",
		saved.Fault.Mode, saved.Fault.AckDelayMs, saved.RejectUnknownGateways, len(saved.Gateways), len(saved.Datapoints),
		saved.MQTT.BrokerURL, saved.MQTT.DataTopic, saved.Influx.Bucket, saved.Influx.RetentionDays)
	writeJSON(w, http.StatusOK, saved)
}

// refreshMeta rewrites datapoint names periodically so the bucket's
// retention never expires them while the readings they label remain.
func (sv *server) refreshMeta(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := sv.influx.writeMeta(ctx, sv.settings.get().Datapoints); err != nil {
			log.Printf("meta refresh: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
