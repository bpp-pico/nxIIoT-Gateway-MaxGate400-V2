package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"nxiiot-gateway/internal/forwarder"
)

func testSettings() Settings {
	return Settings{
		Fault:  FaultSettings{Mode: faultNone},
		MQTT:   MQTTSettings{DataTopic: "gateway/+/data"},
		Influx: InfluxSettings{Bucket: "gateway", RetentionDays: 30},
	}
}

func newTestServer(t *testing.T, s Settings) *server {
	t.Helper()
	st, err := loadSettings("", s)
	if err != nil {
		t.Fatal(err)
	}
	return &server{settings: st, store: newStore(nil), mqtt: &mqttConsumer{}}
}

func setFault(t *testing.T, sv *server, mode string) {
	t.Helper()
	s := sv.settings.get()
	s.Fault.Mode = mode
	if _, err := sv.settings.set(s); err != nil {
		t.Fatal(err)
	}
}

func TestProcessFaultModes(t *testing.T) {
	sv := newTestServer(t, testSettings())
	ctx := context.Background()

	setFault(t, sv, faultErrorAck)
	if r := sv.process(ctx, testMessage()); !r.reply || r.err == "" || len(sv.store.all()) != 0 {
		t.Fatalf("error_ack: %+v stored=%d", r, len(sv.store.all()))
	}
	setFault(t, sv, faultIgnore)
	if r := sv.process(ctx, testMessage()); r.reply || len(sv.store.all()) != 0 {
		t.Fatalf("ignore: %+v stored=%d", r, len(sv.store.all()))
	}
	// A lost ack: stored, no reply; the resend is then a duplicate.
	setFault(t, sv, faultDropAck)
	if r := sv.process(ctx, testMessage()); r.reply || r.accepted != 2 {
		t.Fatalf("drop_ack: %+v", r)
	}
	setFault(t, sv, faultNone)
	if r := sv.process(ctx, testMessage()); !r.reply || r.err != "" || r.dups != 1 || r.accepted != 0 {
		t.Fatalf("resend after lost ack: %+v", r)
	}

	gws, _, _ := sv.store.snapshot()
	if gws[0].Messages != 4 || gws[0].Rejected != 2 || gws[0].DuplicateChunks != 1 {
		t.Fatalf("stats = %+v", gws[0])
	}
}

func TestRejectUnknownGateways(t *testing.T) {
	s := testSettings()
	s.RejectUnknownGateways = true
	s.Gateways = []GatewayEntry{{ID: "GW001", Name: "Line 1"}}
	sv := newTestServer(t, s)

	m := testMessage() // gateway "GW 1" is not registered
	if r := sv.process(context.Background(), m); r.err == "" || r.status != http.StatusForbidden {
		t.Fatalf("unknown gateway: %+v", r)
	}
	m.GatewayID = "GW001"
	if r := sv.process(context.Background(), m); r.err != "" || r.accepted != 2 {
		t.Fatalf("registered gateway: %+v", r)
	}
}

func TestHTTPIngestAndMQTTAck(t *testing.T) {
	sv := newTestServer(t, testSettings())
	h := sv.routes()

	body, _ := json.Marshal(testMessage())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(string(body))))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /ingest = %d %s", rec.Code, rec.Body)
	}

	setFault(t, sv, faultErrorAck)
	var ack forwarder.Ack
	if err := json.Unmarshal(sv.handleMQTT(body), &ack); err != nil {
		t.Fatal(err)
	}
	if ack.ToSeq != 7 || ack.StreamID != "s1" || ack.Error == "" {
		t.Fatalf("ack = %+v, want to_seq 7 with an error", ack)
	}
	setFault(t, sv, faultIgnore)
	if b := sv.handleMQTT(body); b != nil {
		t.Fatalf("ignore mode published an ack: %s", b)
	}
}

func TestSettingsValidateAndPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	st, err := loadSettings(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}

	bad := []func(*Settings){
		func(s *Settings) { s.Fault.Mode = "explode" },
		func(s *Settings) { s.Fault.AckDelayMs = -1 },
		func(s *Settings) { s.MQTT.DataTopic = "gateway/+/ack" },
		func(s *Settings) { s.MQTT.BrokerURL = "emqx:1883 x" },
		func(s *Settings) { s.Influx.Bucket = "_monitoring" },
		func(s *Settings) { s.Gateways = []GatewayEntry{{ID: "A"}, {ID: "A"}} },
	}
	for i, mutate := range bad {
		s := testSettings()
		mutate(&s)
		if _, err := st.set(s); err == nil {
			t.Errorf("bad settings %d accepted", i)
		}
	}

	s := testSettings()
	s.Fault = FaultSettings{Mode: faultDropAck, AckDelayMs: 500}
	s.Gateways = []GatewayEntry{{ID: " GW001 ", Name: "Line 1"}}
	s.Datapoints = []DatapointName{{GatewayID: "GW001", DeviceID: 1, DatapointID: 2, Name: "Voltage", Unit: "V"}, {GatewayID: "GW001", DeviceID: 1, DatapointID: 3}}
	if _, err := st.set(s); err != nil {
		t.Fatal(err)
	}

	again, err := loadSettings(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	got := again.get()
	if got.Fault.Mode != faultDropAck || got.Fault.AckDelayMs != 500 || got.Gateways[0].ID != "GW001" || len(got.Datapoints) != 1 {
		t.Fatalf("reloaded settings = %+v", got)
	}
}
