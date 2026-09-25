package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"nxiiot-gateway/internal/forwarder"
)

func testMessage() forwarder.Message {
	v := 21.5
	return forwarder.Message{
		GatewayID: "GW 1", StreamID: "s1", FromSeq: 7, ToSeq: 7,
		Chunks: []forwarder.WireChunk{{Seq: 7, Readings: []forwarder.WireReading{
			{DeviceID: 1, DatapointID: 2, Timestamp: "2026-09-25T08:00:00.123Z", Value: &v, Quality: "GOOD"},
			{DeviceID: 1, DatapointID: 3, Timestamp: "2026-09-25T15:00:00.456+07:00", Value: nil, Quality: "TIMEOUT"},
		}}},
	}
}

func TestLineProtocol(t *testing.T) {
	m := testMessage()
	b, err := lineProtocol(m.GatewayID, m.StreamID, m.Chunks)
	if err != nil {
		t.Fatal(err)
	}
	want := `reading,gateway_id=GW\ 1,device_id=1,datapoint_id=2 value=21.5,quality="GOOD",seq=7i,stream_id="s1" 1790323200123` + "\n" +
		`reading,gateway_id=GW\ 1,device_id=1,datapoint_id=3 quality="TIMEOUT",seq=7i,stream_id="s1" 1790323200456` + "\n"
	if got := b.String(); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

// fakeInflux records write bodies and fails while fail is set.
type fakeInflux struct {
	srv    *httptest.Server
	fail   bool
	bodies []string
	bucket []string
}

func newFakeInflux(t *testing.T) *fakeInflux {
	f := &fakeInflux{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Token tok" {
			t.Errorf("Authorization = %q", got)
		}
		if f.fail {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		b, _ := io.ReadAll(r.Body)
		f.bodies = append(f.bodies, string(b))
		f.bucket = append(f.bucket, r.URL.Query().Get("bucket"))
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestIngestWritesInfluxAndRejectsOnFailure(t *testing.T) {
	f := newFakeInflux(t)
	f.fail = true
	st := newStore(newInfluxWriter(f.srv.URL, "org", "tok", func() string { return "bucket" }))
	ctx := context.Background()

	if _, _, err := st.ingest(ctx, testMessage()); err == nil {
		t.Fatal("expected an error while InfluxDB fails")
	}
	if len(st.all()) != 0 {
		t.Fatal("a rejected message must not be marked as received")
	}

	f.fail = false
	accepted, dups, err := st.ingest(ctx, testMessage())
	if err != nil || accepted != 2 || dups != 0 {
		t.Fatalf("resend: accepted=%d dups=%d err=%v", accepted, dups, err)
	}
	accepted, dups, err = st.ingest(ctx, testMessage())
	if err != nil || accepted != 0 || dups != 1 {
		t.Fatalf("duplicate: accepted=%d dups=%d err=%v", accepted, dups, err)
	}
	if len(f.bodies) != 1 || strings.Count(f.bodies[0], "\n") != 2 || f.bucket[0] != "bucket" {
		t.Fatalf("influx writes = %q to %q, want one write of 2 lines to bucket", f.bodies, f.bucket)
	}
}

func TestGapIsCountedAndWritten(t *testing.T) {
	f := newFakeInflux(t)
	st := newStore(newInfluxWriter(f.srv.URL, "org", "tok", func() string { return "b" }))
	ctx := context.Background()
	for _, seq := range []int64{1, 2, 6} {
		m := testMessage()
		m.GatewayID, m.FromSeq, m.ToSeq, m.Chunks[0].Seq = "GW", seq, seq, seq
		if _, _, err := st.ingest(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	gws, dps, gaps := st.snapshot()
	if len(gaps) != 1 || gaps[0].FromSeq != 3 || gaps[0].ToSeq != 5 {
		t.Fatalf("gaps = %+v, want 3-5", gaps)
	}
	if gws[0].Gaps != 1 || gws[0].MissingChunks != 3 || gws[0].Streams["s1"] != 6 || gws[0].Readings != 6 {
		t.Fatalf("stats = %+v", gws[0])
	}
	if len(dps) != 2 {
		t.Fatalf("datapoints = %+v, want 2", dps)
	}
	var gapLine bool
	for _, b := range f.bodies {
		gapLine = gapLine || strings.HasPrefix(b, "gap,gateway_id=GW from_seq=3i,to_seq=5i,chunks=3i")
	}
	if !gapLine {
		t.Fatalf("no gap point written: %q", f.bodies)
	}
}

func TestEnsureBucketCreatesMissingBucket(t *testing.T) {
	var created map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/buckets":
			http.Error(w, `{"code":"not found"}`, http.StatusNotFound) // what InfluxDB 2.7 does
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/orgs":
			w.Write([]byte(`{"orgs":[{"id":"org1"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/buckets":
			json.NewDecoder(r.Body).Decode(&created)
			w.WriteHeader(http.StatusCreated)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
		}
	}))
	defer srv.Close()

	w := newInfluxWriter(srv.URL, "org", "tok", func() string { return "b" })
	if err := w.ensureBucket(context.Background(), "new", 7); err != nil {
		t.Fatal(err)
	}
	if created["name"] != "new" || created["orgID"] != "org1" {
		t.Fatalf("created = %v", created)
	}
}
