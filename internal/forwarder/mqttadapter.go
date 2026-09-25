package forwarder

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MQTTAdapterConfig configures MQTTAdapter. Durations and a resolved
// *tls.Config are used here (rather than config.MQTTConfig's raw
// yaml-facing ints/paths) the same way forwarder.Config translates
// config.ForwarderConfig — the translation happens once, in main.go.
type MQTTAdapterConfig struct {
	BrokerURL      string
	ClientID       string
	Username       string
	Password       string
	QoS            byte
	DataTopic      string
	AckTopic       string
	KeepAlive      time.Duration
	ConnectTimeout time.Duration
	PublishTimeout time.Duration
	// AckTimeout bounds how long Send waits for the application-level ack
	// (§15) after the broker has accepted the publish. A message that times
	// out here is reported as a failed Send and resent like any other
	// failure — the server may still have stored it, but de-duplication on
	// (gateway_id, stream_id, seq) makes that safe.
	AckTimeout time.Duration
	TLS        *tls.Config // nil disables TLS

	// WatchdogInterval and ReconnectStuckAfter tune RunReconnectWatchdog
	// (see its doc comment) — zero on either defaults to the production
	// values (15s / 45s) in NewMQTTAdapter. Exposed so tests can shrink
	// them to make the stuck-reconnect path observable in well under a
	// second instead of the real 45s.
	WatchdogInterval    time.Duration
	ReconnectStuckAfter time.Duration
}

const (
	defaultWatchdogInterval    = 15 * time.Second
	defaultReconnectStuckAfter = 45 * time.Second
)

// MQTTAdapter is the production Adapter (§15): publishes each Message to
// DataTopic at QoS 1 and waits for an application-level Ack on AckTopic
// whose to_seq matches — QoS 1's PUBACK only confirms the broker received
// the message, not that the server stored it. Connect/reconnect is
// delegated to paho's built-in auto-reconnect.
type MQTTAdapter struct {
	client mqtt.Client
	cfg    MQTTAdapterConfig
	log    *slog.Logger

	mu sync.Mutex
	// pending maps a message's to_seq to the channel its Send waits on.
	pending map[int64]chan Ack
	// streamID of the message awaiting each ack, to ignore an ack that
	// names a different stream.
	pendingStream map[int64]string
	// disconnectedAt is zero only in the instant right after onConnect
	// fires — NewMQTTAdapter sets it to time.Now() immediately so a fresh
	// client that has never connected at all counts as disconnected from
	// construction, not as the zero value's naive reading of "connected".
	// Getting this wrong is what let a never-successfully-connected client
	// slip past checkAndForceReconnect's IsZero() check forever (see
	// RunReconnectWatchdog's doc comment and MEMORY.md's 2026-09-0X entry
	// on the initial-connect-fatal fix this enables) — the watchdog only
	// ever exercised the "was connected, then lost it" path before.
	disconnectedAt time.Time
}

func NewMQTTAdapter(cfg MQTTAdapterConfig, log *slog.Logger) *MQTTAdapter {
	if cfg.WatchdogInterval <= 0 {
		cfg.WatchdogInterval = defaultWatchdogInterval
	}
	if cfg.ReconnectStuckAfter <= 0 {
		cfg.ReconnectStuckAfter = defaultReconnectStuckAfter
	}
	a := &MQTTAdapter{cfg: cfg, log: log, pending: make(map[int64]chan Ack), pendingStream: make(map[int64]string), disconnectedAt: time.Now()}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID(cfg.ClientID).
		SetKeepAlive(cfg.KeepAlive).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetOrderMatters(false).
		SetOnConnectHandler(a.onConnect).
		SetConnectionLostHandler(a.onConnectionLost)

	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}
	if cfg.TLS != nil {
		opts.SetTLSConfig(cfg.TLS)
	}

	a.client = mqtt.NewClient(opts)
	return a
}

// Connect blocks until the initial connection succeeds, ctx is done, or
// ConnectTimeout elapses. After this call returns successfully, paho's
// AutoReconnect (§15 "Reconnect") keeps the session alive on its own —
// Send simply fails while disconnected, and the Forwarder retries.
func (a *MQTTAdapter) Connect(ctx context.Context) error {
	token := a.client.Connect()
	if err := waitToken(ctx, token, a.cfg.ConnectTimeout); err != nil {
		return fmt.Errorf("mqtt connect: %w", err)
	}
	return nil
}

func (a *MQTTAdapter) Disconnect(quiesceMs uint) {
	a.client.Disconnect(quiesceMs)
}

func (a *MQTTAdapter) IsConnected() bool {
	return a.client.IsConnectionOpen()
}

func (a *MQTTAdapter) onConnect(c mqtt.Client) {
	a.log.Info("mqtt connected", "broker", a.cfg.BrokerURL, "client_id", a.cfg.ClientID)
	a.mu.Lock()
	a.disconnectedAt = time.Time{}
	a.mu.Unlock()
	go a.subscribeWithRetry(c)
}

// subscribeWithRetry mirrors internal-server/consumer.go's subscribeWithRetry
// — a subscribe that loses the race with a fresh disconnect is retried up to
// 3 times with a short backoff before giving up and waiting for the next
// reconnect, rather than only logging a single failure (the gap this closes:
// a real log had exactly one subscribe failure with no follow-up attempt,
// leaving the ack topic unsubscribed until the next full reconnect).
func (a *MQTTAdapter) subscribeWithRetry(c mqtt.Client) {
	for attempt := 1; attempt <= 3; attempt++ {
		token := c.Subscribe(a.cfg.AckTopic, a.cfg.QoS, a.handleAck)
		token.Wait()
		if token.Error() == nil {
			return
		}
		a.log.Error("mqtt: failed to subscribe to ack topic, retrying", "topic", a.cfg.AckTopic, "attempt", attempt, "error", token.Error())
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	a.log.Error("mqtt: failed to subscribe to ack topic after 3 attempts; waiting for next reconnect", "topic", a.cfg.AckTopic)
}

func (a *MQTTAdapter) onConnectionLost(_ mqtt.Client, err error) {
	a.log.Warn("mqtt connection lost, will auto-reconnect", "error", err)
	a.mu.Lock()
	a.disconnectedAt = time.Now()
	a.mu.Unlock()
}

// RunReconnectWatchdog is the automated version of the fix for a real
// 2026-08-27 incident: paho's own AutoReconnect (SetAutoReconnect(true) +
// SetConnectRetry(true), 5s retry interval — see NewMQTTAdapter) is
// supposed to recover the connection on its own after onConnectionLost,
// but was observed to silently stall after a "pingresp not received"
// disconnect — the broker stayed reachable throughout (confirmed
// independently with a direct TCP check), yet the client sat disconnected
// for 7+ minutes with zero further log activity until the gateway process
// was restarted by hand. That restart is the only thing that actually
// unstuck it, which is exactly what this watchdog now does automatically,
// scoped to just the MQTT client rather than the whole process.
//
// It intentionally waits ReconnectStuckAfter (default 45s — several
// multiples of paho's own 5s retry interval) before concluding the
// built-in retry has stalled rather than merely being slow, so a broker
// that's genuinely still unreachable isn't hammered by two overlapping
// reconnect loops. Runs until ctx is cancelled (the gateway's top-level
// shutdown context — see cmd/gateway/adapter.go).
func (a *MQTTAdapter) RunReconnectWatchdog(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.WatchdogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.checkAndForceReconnect()
		}
	}
}

// isPahoAlreadyReconnectingErr reports whether err is paho's internal
// status-transition guard rejecting Client.Connect() because its own
// AutoReconnect/ConnectRetry goroutine already moved the client out of the
// "disconnected" state on its own. Matched by message because
// paho.mqtt.golang (pinned at v1.5.1, see go.mod) keeps this as an
// unexported sentinel (errStatusMustBeDisconnected in its status.go) rather
// than a public API — re-verify this string against that file if the paho
// dependency version ever changes.
func isPahoAlreadyReconnectingErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "status can only transition to connecting from disconnected")
}

func (a *MQTTAdapter) checkAndForceReconnect() {
	if a.client.IsConnectionOpen() {
		return
	}

	a.mu.Lock()
	stuckSince := a.disconnectedAt
	a.mu.Unlock()
	if stuckSince.IsZero() || time.Since(stuckSince) < a.cfg.ReconnectStuckAfter {
		return
	}

	a.log.Warn("mqtt client disconnected too long, forcing reconnect", "disconnected_for", time.Since(stuckSince))
	token := a.client.Connect()
	token.WaitTimeout(a.cfg.ConnectTimeout)
	if err := token.Error(); err != nil {
		if isPahoAlreadyReconnectingErr(err) {
			// paho's own AutoReconnect/ConnectRetry goroutine is already
			// mid-attempt (its internal status is "connecting" or
			// "reconnecting", not "disconnected") -- this is proof paho is
			// actively retrying on its own, not the silent-stall scenario
			// this watchdog exists to fix (see doc comment above). This
			// used to be misclassified as a failure, and worse, was
			// preceded by an unconditional client.Disconnect(250) call that
			// tore down paho's legitimate in-flight attempt every single
			// tick -- observed live on 2026-09-02 forcing ~9 hours of
			// otherwise-healthy paho retries to restart from scratch once
			// a minute, every attempt rejected with
			// "status can only transition to connecting from disconnected"
			// (see MEMORY.md). Removed the preemptive Disconnect() and
			// treat this specific error as a no-op instead of an error.
			a.log.Info("mqtt: paho already reconnecting on its own, watchdog took no action", "disconnected_for", time.Since(stuckSince))
		} else {
			a.log.Error("mqtt: forced reconnect attempt failed, will retry at next watchdog tick", "error", err)
		}
	}

	if !a.client.IsConnectionOpen() {
		// Still down — push the deadline out so the next forced attempt
		// waits a full ReconnectStuckAfter rather than retrying every
		// tick against a broker that's genuinely still unreachable. If it
		// DID come back up, onConnect already zeroed disconnectedAt and
		// this is skipped, so a fast recovery isn't mislabeled as stuck.
		a.mu.Lock()
		a.disconnectedAt = time.Now()
		a.mu.Unlock()
	}
}

func (a *MQTTAdapter) handleAck(_ mqtt.Client, msg mqtt.Message) {
	var ack Ack
	if err := json.Unmarshal(msg.Payload(), &ack); err != nil {
		a.log.Warn("mqtt: invalid ack payload", "error", err)
		return
	}

	a.mu.Lock()
	ch, ok := a.pending[ack.ToSeq]
	stream := a.pendingStream[ack.ToSeq]
	a.mu.Unlock()
	if !ok || (ack.StreamID != "" && ack.StreamID != stream) {
		// Late ack for a message Send already gave up on, a duplicate,
		// or an ack for another stream — nothing is waiting for it.
		return
	}
	select {
	case ch <- ack:
	default: // a duplicate ack already delivered; don't block paho's handler
	}
}

// Send publishes msg to DataTopic at QoS 1 and waits for the matching
// application-level ack on AckTopic (§15). Duplicate handling is the
// server's job, via (gateway_id, stream_id, seq), as with HTTPAdapter.
func (a *MQTTAdapter) Send(ctx context.Context, msg Message) error {
	if !a.IsConnected() {
		return fmt.Errorf("mqtt: not connected to %s", a.cfg.BrokerURL)
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	ackCh := make(chan Ack, 1)
	a.mu.Lock()
	a.pending[msg.ToSeq] = ackCh
	a.pendingStream[msg.ToSeq] = msg.StreamID
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.pending, msg.ToSeq)
		delete(a.pendingStream, msg.ToSeq)
		a.mu.Unlock()
	}()

	token := a.client.Publish(a.cfg.DataTopic, a.cfg.QoS, false, body)
	if err := waitToken(ctx, token, a.cfg.PublishTimeout); err != nil {
		return fmt.Errorf("mqtt publish: %w", err)
	}

	select {
	case ack := <-ackCh:
		if ack.Error != "" {
			return fmt.Errorf("server rejected seq %d-%d: %s", msg.FromSeq, msg.ToSeq, ack.Error)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(a.cfg.AckTimeout):
		return fmt.Errorf("mqtt: timed out waiting for ack of seq %d-%d", msg.FromSeq, msg.ToSeq)
	}
}

// waitToken blocks until token completes, ctx is done, or timeout elapses.
func waitToken(ctx context.Context, token mqtt.Token, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		token.Wait()
		close(done)
	}()

	select {
	case <-done:
		return token.Error()
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return fmt.Errorf("timed out after %s", timeout)
	}
}
