package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HTTPAdapter is a minimal Adapter implementation used for local dev/test
// (cmd/server-sim) and as the fallback transport. §15 requires the
// transport to sit behind this interface specifically so it can be swapped
// for MQTTAdapter without touching the forwarding loop.
type HTTPAdapter struct {
	url    string
	client *http.Client
}

func NewHTTPAdapter(url string, timeout time.Duration) *HTTPAdapter {
	return &HTTPAdapter{url: url, client: &http.Client{Timeout: timeout}}
}

// Send POSTs msg as JSON; any 2xx response counts as the ack.
func (a *HTTPAdapter) Send(ctx context.Context, msg Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server returned %s", resp.Status)
	}
	return nil
}
