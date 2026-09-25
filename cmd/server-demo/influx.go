package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"nxiiot-gateway/internal/forwarder"
)

// influxWriter talks to InfluxDB 2.x over its plain HTTP API (line protocol
// for writes), so server-demo needs no client library. The bucket comes from
// the current settings on every call, so a change on the config page takes
// effect on the next write.
//
// Measurements (see test-server/README.md):
//   - reading: one point per reading, tagged gateway_id/device_id/datapoint_id,
//     timestamped with the reading's own ts (ms). InfluxDB overwrites a point
//     with the same series and timestamp, so writing a chunk twice stores it once.
//   - gap: one point per seq range missing from a stream.
//   - datapoint_meta: the name/unit set on the config page, rewritten on every
//     change and periodically so the bucket's retention never expires it.
type influxWriter struct {
	baseURL, org, token string
	bucket              func() string
	client              *http.Client

	mu      sync.Mutex
	lastErr string
}

func newInfluxWriter(baseURL, org, token string, bucket func() string) *influxWriter {
	return &influxWriter{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		org:     org,
		token:   token,
		bucket:  bucket,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (w *influxWriter) writeChunks(ctx context.Context, gatewayID, streamID string, chunks []forwarder.WireChunk) error {
	body, err := lineProtocol(gatewayID, streamID, chunks)
	if err != nil {
		return err
	}
	return w.writeLines(ctx, body)
}

func (w *influxWriter) writeGap(ctx context.Context, g gap) error {
	var b bytes.Buffer
	fmt.Fprintf(&b, "gap,gateway_id=%s from_seq=%di,to_seq=%di,chunks=%di,stream_id=\"%s\" %d\n",
		escapeTag(g.GatewayID), g.FromSeq, g.ToSeq, g.ToSeq-g.FromSeq+1, escapeFieldString(g.StreamID), g.At.UnixMilli())
	return w.writeLines(ctx, &b)
}

// writeMeta rewrites the name/unit of every named datapoint, stamped now.
func (w *influxWriter) writeMeta(ctx context.Context, dps []DatapointName) error {
	var b bytes.Buffer
	now := time.Now().UnixMilli()
	for _, d := range dps {
		fmt.Fprintf(&b, "datapoint_meta,gateway_id=%s,device_id=%d,datapoint_id=%d name=\"%s\",unit=\"%s\" %d\n",
			escapeTag(d.GatewayID), d.DeviceID, d.DatapointID, escapeFieldString(d.Name), escapeFieldString(d.Unit), now)
	}
	return w.writeLines(ctx, &b)
}

func (w *influxWriter) writeLines(ctx context.Context, body *bytes.Buffer) error {
	if body.Len() == 0 {
		return nil
	}
	q := url.Values{"org": {w.org}, "bucket": {w.bucket()}, "precision": {"ms"}}
	err := w.do(ctx, http.MethodPost, "/api/v2/write?"+q.Encode(), "text/plain; charset=utf-8", body, nil)
	w.mu.Lock()
	w.lastErr = ""
	if err != nil {
		w.lastErr = err.Error()
	}
	w.mu.Unlock()
	return err
}

func (w *influxWriter) lastError() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastErr
}

// ensureBucket creates bucket if it is missing and sets its retention.
func (w *influxWriter) ensureBucket(ctx context.Context, bucket string, retentionDays int) error {
	rules := []map[string]any{}
	if retentionDays > 0 {
		rules = append(rules, map[string]any{"type": "expire", "everySeconds": retentionDays * 86400})
	}

	var found struct {
		Buckets []struct {
			ID string `json:"id"`
		} `json:"buckets"`
	}
	q := url.Values{"org": {w.org}, "name": {bucket}}
	// A missing bucket is a 404, not an empty list.
	if err := w.do(ctx, http.MethodGet, "/api/v2/buckets?"+q.Encode(), "", nil, &found); err != nil && !isNotFound(err) {
		return err
	}
	if len(found.Buckets) > 0 {
		body, _ := json.Marshal(map[string]any{"retentionRules": rules})
		return w.do(ctx, http.MethodPatch, "/api/v2/buckets/"+found.Buckets[0].ID, "application/json", bytes.NewReader(body), nil)
	}

	var orgs struct {
		Orgs []struct {
			ID string `json:"id"`
		} `json:"orgs"`
	}
	if err := w.do(ctx, http.MethodGet, "/api/v2/orgs?"+url.Values{"org": {w.org}}.Encode(), "", nil, &orgs); err != nil {
		return err
	}
	if len(orgs.Orgs) == 0 {
		return fmt.Errorf("influx org %q not found", w.org)
	}
	body, _ := json.Marshal(map[string]any{"orgID": orgs.Orgs[0].ID, "name": bucket, "retentionRules": rules})
	return w.do(ctx, http.MethodPost, "/api/v2/buckets", "application/json", bytes.NewReader(body), nil)
}

func (w *influxWriter) do(ctx context.Context, method, path, contentType string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, w.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+w.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("influx %s: %w", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &influxError{status: resp.StatusCode, msg: fmt.Sprintf("influx %s %s: %s: %s", method, strings.SplitN(path, "?", 2)[0], resp.Status, strings.TrimSpace(string(msg)))}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

type influxError struct {
	status int
	msg    string
}

func (e *influxError) Error() string { return e.msg }

func isNotFound(err error) bool {
	var ie *influxError
	return errors.As(err, &ie) && ie.status == http.StatusNotFound
}

// lineProtocol renders chunks as InfluxDB line protocol, e.g.
//
//	reading,gateway_id=GW001,device_id=1,datapoint_id=2 value=21.5,quality="GOOD",seq=7i,stream_id="…" 1758787200123
//
// A null value (a failed read) omits the value field; quality is always set.
func lineProtocol(gatewayID, streamID string, chunks []forwarder.WireChunk) (*bytes.Buffer, error) {
	var b bytes.Buffer
	gw := escapeTag(gatewayID)
	stream := escapeFieldString(streamID)
	for _, c := range chunks {
		for _, r := range c.Readings {
			ts, err := time.Parse(time.RFC3339Nano, r.Timestamp)
			if err != nil {
				return nil, fmt.Errorf("chunk %d: bad ts %q: %w", c.Seq, r.Timestamp, err)
			}
			b.WriteString("reading,gateway_id=")
			b.WriteString(gw)
			b.WriteString(",device_id=")
			b.WriteString(strconv.FormatInt(r.DeviceID, 10))
			b.WriteString(",datapoint_id=")
			b.WriteString(strconv.FormatInt(r.DatapointID, 10))
			b.WriteByte(' ')
			if r.Value != nil {
				b.WriteString("value=")
				b.WriteString(strconv.FormatFloat(*r.Value, 'g', -1, 64))
				b.WriteByte(',')
			}
			b.WriteString(`quality="`)
			b.WriteString(escapeFieldString(r.Quality))
			b.WriteString(`",seq=`)
			b.WriteString(strconv.FormatInt(c.Seq, 10))
			b.WriteString(`i,stream_id="`)
			b.WriteString(stream)
			b.WriteString(`" `)
			b.WriteString(strconv.FormatInt(ts.UnixMilli(), 10))
			b.WriteByte('\n')
		}
	}
	return &b, nil
}

var tagEscaper = strings.NewReplacer(",", `\,`, "=", `\=`, " ", `\ `)

func escapeTag(s string) string { return tagEscaper.Replace(s) }

// Line protocol has no escape for a newline in a string field, so it becomes a space.
var fieldStringEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", " ", "\r", " ")

func escapeFieldString(s string) string { return fieldStringEscaper.Replace(s) }
