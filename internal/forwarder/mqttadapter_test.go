package forwarder_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"nxiiot-gateway/internal/forwarder"
)

// syncBuffer is a concurrency-safe io.Writer for asserting on log output
// written from a background goroutine (RunReconnectWatchdog) while the
// test goroutine reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startStoppableTestBroker is startTestBroker's sibling for tests that need
// to force a real disconnect mid-test (rather than only at t.Cleanup) —
// the watchdog tests need the broker to actually go away while the
// adapter is connected to it, not a hand-rolled fake disconnect.
func startStoppableTestBroker(t *testing.T) (brokerURL string, stop func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	server := mochi.New(nil)
	if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("add allow-all hook: %v", err)
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "test", Address: addr})
	if err := server.AddListener(tcp); err != nil {
		t.Fatalf("add tcp listener: %v", err)
	}
	go func() { _ = server.Serve() }()

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		_ = server.Close()
	}
	t.Cleanup(stop)

	return "tcp://" + addr, stop
}

// startTestBroker runs a real, embedded, pure-Go MQTT broker for the
// duration of the test — matching this package's existing preference
// (openTestRepo, forwarder_test.go) for exercising real infrastructure
// over hand-rolled fakes. It returns the "tcp://host:port" URL to connect
// to.
func startTestBroker(t *testing.T) string {
	t.Helper()

	// Reserve a free port, then hand it to mochi — mochi's own listener
	// binds it a moment later, which is an accepted small race for tests.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	server := mochi.New(nil)
	if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("add allow-all hook: %v", err)
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "test", Address: addr})
	if err := server.AddListener(tcp); err != nil {
		t.Fatalf("add tcp listener: %v", err)
	}

	go func() {
		_ = server.Serve()
	}()
	t.Cleanup(func() { _ = server.Close() })

	return "tcp://" + addr
}

func newTestMQTTAdapter(t *testing.T, brokerURL string, ackTimeout time.Duration) *forwarder.MQTTAdapter {
	t.Helper()
	adapter := forwarder.NewMQTTAdapter(forwarder.MQTTAdapterConfig{
		BrokerURL:      brokerURL,
		ClientID:       "GW001-adapter",
		QoS:            1,
		DataTopic:      "gateway/GW001/data",
		AckTopic:       "gateway/GW001/ack",
		KeepAlive:      10 * time.Second,
		ConnectTimeout: 2 * time.Second,
		PublishTimeout: 2 * time.Second,
		AckTimeout:     ackTimeout,
	}, newTestLogger(t))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("adapter.Connect: %v", err)
	}
	t.Cleanup(func() { adapter.Disconnect(100) })
	return adapter
}

// startFakeInternalServer stands in for the real Internal Server's MQTT
// consumer (cmd/server-demo's role, but over MQTT): it subscribes to
// dataTopic and, for every batch received, publishes an ack to ackTopic —
// exercising the adapter's application-level ack round trip for real,
// not by asserting on internal adapter state.
func startFakeInternalServer(t *testing.T, brokerURL, dataTopic, ackTopic string) <-chan forwarder.Message {
	t.Helper()
	received := make(chan forwarder.Message, 10)

	opts := pahomqtt.NewClientOptions().AddBroker(brokerURL).SetClientID("fake-internal-server")
	opts.SetDefaultPublishHandler(func(client pahomqtt.Client, msg pahomqtt.Message) {
		var m forwarder.Message
		if err := json.Unmarshal(msg.Payload(), &m); err != nil {
			t.Errorf("fake server: invalid message payload: %v", err)
			return
		}
		received <- m

		ack, _ := json.Marshal(forwarder.Ack{StreamID: m.StreamID, ToSeq: m.ToSeq})
		client.Publish(ackTopic, 1, false, ack)
	})

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("fake server connect: %v", token.Error())
	}
	if token := client.Subscribe(dataTopic, 1, nil); token.Wait() && token.Error() != nil {
		t.Fatalf("fake server subscribe: %v", token.Error())
	}
	t.Cleanup(func() { client.Disconnect(100) })

	return received
}

func sampleMessage() forwarder.Message {
	v := 230.2
	return forwarder.Message{
		GatewayID: "GW001", StreamID: "stream-1", FromSeq: 7, ToSeq: 7,
		Chunks: []forwarder.WireChunk{{Seq: 7, Readings: []forwarder.WireReading{
			{DeviceID: 1, DatapointID: 1, Timestamp: "2026-09-25T08:00:00.000Z", Value: &v, Quality: "GOOD"},
		}}},
	}
}

func TestMQTTAdapterSendSucceedsWhenServerAcks(t *testing.T) {
	brokerURL := startTestBroker(t)
	adapter := newTestMQTTAdapter(t, brokerURL, 3*time.Second)
	received := startFakeInternalServer(t, brokerURL, "gateway/GW001/data", "gateway/GW001/ack")

	// Give the fake server's subscription a moment to register with the
	// broker before the adapter publishes.
	time.Sleep(200 * time.Millisecond)

	if !adapter.IsConnected() {
		t.Fatal("expected adapter to report connected after Connect")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := adapter.Send(ctx, sampleMessage()); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case got := <-received:
		if got.GatewayID != "GW001" || got.ToSeq != 7 || got.ReadingCount() != 1 || *got.Chunks[0].Readings[0].Value != 230.2 {
			t.Errorf("fake server received unexpected message: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("fake server never received the published message")
	}
}

func TestMQTTAdapterSendFailsWhenAckNeverArrives(t *testing.T) {
	brokerURL := startTestBroker(t)
	// No fake server subscribed — nothing will ever ack.
	adapter := newTestMQTTAdapter(t, brokerURL, 300*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := adapter.Send(ctx, sampleMessage())
	if err == nil {
		t.Fatal("expected Send to fail when no application-level ack arrives")
	}
}

// TestMQTTAdapterWatchdogForcesReconnectWhenStuckPastThreshold reproduces
// the shape of the real 2026-08-27 incident: the client is connected, then
// genuinely disconnected (the broker actually goes away — not a simulated
// event), and stays disconnected past ReconnectStuckAfter. The watchdog
// must notice and force a reconnect attempt rather than trusting paho's
// own AutoReconnect indefinitely.
// TestMQTTAdapterWatchdogActsOnAClientThatNeverConnected guards the
// 2026-09-04 fix enabling cmd/gateway/adapter.go's initial-connect-not-
// fatal change (see MEMORY.md): before it, disconnectedAt's zero Go value
// was misread by checkAndForceReconnect as "currently connected" for any
// client that had never once connected (onConnectionLost, the only other
// place that set it, never fires for a connect that never succeeded in the
// first place) — so the watchdog silently took no action for a client
// stuck in exactly that state, no matter how long you waited.
// NewMQTTAdapter now initializes disconnectedAt to time.Now() so this
// state is indistinguishable from "was connected, then lost it" as far as
// the watchdog is concerned.
//
// Matches TestMQTTAdapterWatchdogForcesReconnectWhenStuckPastThreshold's
// assertion style (a log line, not necessarily full reconnection) rather
// than asserting IsConnected() — paho's own Connect() spawns its own
// internal retry loop on a hardcoded 5s ConnectRetryInterval
// (client.go's RETRYCONN, independent of this watchdog) regardless of
// whether this fix exists, so actually reaching "connected" within a
// short test deadline isn't a reliable signal of *this* fix specifically;
// the watchdog even noticing and attempting to act (the log line) is.
func TestMQTTAdapterWatchdogActsOnAClientThatNeverConnected(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close() // freed; nothing ever listens here in this test

	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	adapter := forwarder.NewMQTTAdapter(forwarder.MQTTAdapterConfig{
		BrokerURL:           "tcp://" + addr,
		ClientID:            "GW001-never-connected-test",
		QoS:                 1,
		DataTopic:           "gateway/GW001/data",
		AckTopic:            "gateway/GW001/ack",
		KeepAlive:           10 * time.Second,
		ConnectTimeout:      200 * time.Millisecond,
		PublishTimeout:      time.Second,
		AckTimeout:          time.Second,
		WatchdogInterval:    30 * time.Millisecond,
		ReconnectStuckAfter: 100 * time.Millisecond,
	}, logger)

	// Mirrors buildAdapter's new behavior exactly: the initial Connect
	// fails and that failure is deliberately ignored (not fatal), same as
	// production now does. onConnect/onConnectionLost never fire either
	// way — this client has never once been connected.
	connectCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	_ = adapter.Connect(connectCtx)
	cancel()
	if adapter.IsConnected() {
		t.Fatal("expected the initial connect against an empty port to fail")
	}

	watchdogCtx, stopWatchdog := context.WithCancel(context.Background())
	defer stopWatchdog()
	defer adapter.Disconnect(100)
	go adapter.RunReconnectWatchdog(watchdogCtx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logBuf.String(), "forcing reconnect") {
			return // watchdog noticed a never-connected client and acted — test passes
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected the watchdog to act on a client that had never connected within 2s, got log:\n%s", logBuf.String())
}

func TestMQTTAdapterWatchdogForcesReconnectWhenStuckPastThreshold(t *testing.T) {
	brokerURL, stopBroker := startStoppableTestBroker(t)

	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	adapter := forwarder.NewMQTTAdapter(forwarder.MQTTAdapterConfig{
		BrokerURL:           brokerURL,
		ClientID:            "GW001-watchdog-test",
		QoS:                 1,
		DataTopic:           "gateway/GW001/data",
		AckTopic:            "gateway/GW001/ack",
		KeepAlive:           10 * time.Second,
		ConnectTimeout:      500 * time.Millisecond,
		PublishTimeout:      time.Second,
		AckTimeout:          time.Second,
		WatchdogInterval:    30 * time.Millisecond,
		ReconnectStuckAfter: 100 * time.Millisecond,
	}, logger)

	connectCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := adapter.Connect(connectCtx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer adapter.Disconnect(100)

	if !adapter.IsConnected() {
		t.Fatal("expected adapter connected before stopping the broker")
	}

	watchdogCtx, stopWatchdog := context.WithCancel(context.Background())
	defer stopWatchdog()
	go adapter.RunReconnectWatchdog(watchdogCtx)

	stopBroker() // real disconnect — the broker is genuinely gone

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logBuf.String(), "forcing reconnect") {
			return // watchdog noticed and acted — test passes
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected watchdog to log a forced reconnect attempt within 2s, got log:\n%s", logBuf.String())
}

// TestMQTTAdapterWatchdogWaitsOutTheStuckWindow guards against the
// watchdog being trigger-happy: a disconnect that hasn't yet sat past
// ReconnectStuckAfter must not be force-reconnected — that would fight
// paho's own (still-legitimate) retry attempts instead of only stepping
// in once they've genuinely stalled.
func TestMQTTAdapterWatchdogWaitsOutTheStuckWindow(t *testing.T) {
	brokerURL, stopBroker := startStoppableTestBroker(t)

	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	adapter := forwarder.NewMQTTAdapter(forwarder.MQTTAdapterConfig{
		BrokerURL:           brokerURL,
		ClientID:            "GW001-watchdog-test-2",
		QoS:                 1,
		DataTopic:           "gateway/GW001/data",
		AckTopic:            "gateway/GW001/ack",
		KeepAlive:           10 * time.Second,
		ConnectTimeout:      500 * time.Millisecond,
		PublishTimeout:      time.Second,
		AckTimeout:          time.Second,
		WatchdogInterval:    20 * time.Millisecond,
		ReconnectStuckAfter: 5 * time.Second, // deliberately long relative to the test's own window
	}, logger)

	connectCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := adapter.Connect(connectCtx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer adapter.Disconnect(100)

	watchdogCtx, stopWatchdog := context.WithCancel(context.Background())
	defer stopWatchdog()
	go adapter.RunReconnectWatchdog(watchdogCtx)

	stopBroker()
	time.Sleep(300 * time.Millisecond) // several watchdog ticks, nowhere near ReconnectStuckAfter

	if strings.Contains(logBuf.String(), "forcing reconnect") {
		t.Fatalf("watchdog acted before ReconnectStuckAfter elapsed, got log:\n%s", logBuf.String())
	}
}

// TestMQTTAdapterWatchdogLeavesAHealthyConnectionAlone is the base case:
// while genuinely connected, the watchdog must never intervene, no matter
// how many ticks pass.
func TestMQTTAdapterWatchdogLeavesAHealthyConnectionAlone(t *testing.T) {
	brokerURL := startTestBroker(t)

	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	adapter := forwarder.NewMQTTAdapter(forwarder.MQTTAdapterConfig{
		BrokerURL:           brokerURL,
		ClientID:            "GW001-watchdog-test-3",
		QoS:                 1,
		DataTopic:           "gateway/GW001/data",
		AckTopic:            "gateway/GW001/ack",
		KeepAlive:           10 * time.Second,
		ConnectTimeout:      500 * time.Millisecond,
		PublishTimeout:      time.Second,
		AckTimeout:          time.Second,
		WatchdogInterval:    20 * time.Millisecond,
		ReconnectStuckAfter: 50 * time.Millisecond,
	}, logger)

	connectCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := adapter.Connect(connectCtx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer adapter.Disconnect(100)

	watchdogCtx, stopWatchdog := context.WithCancel(context.Background())
	defer stopWatchdog()
	go adapter.RunReconnectWatchdog(watchdogCtx)

	time.Sleep(300 * time.Millisecond) // many ticks, well past ReconnectStuckAfter, connection never drops

	if !adapter.IsConnected() {
		t.Fatal("expected the connection to remain healthy for the duration of this test")
	}
	if strings.Contains(logBuf.String(), "forcing reconnect") {
		t.Fatalf("watchdog acted on a healthy connection, got log:\n%s", logBuf.String())
	}
}

func TestMQTTAdapterSendFailsFastWhenNotConnected(t *testing.T) {
	brokerURL := startTestBroker(t)
	adapter := newTestMQTTAdapter(t, brokerURL, 5*time.Second)
	adapter.Disconnect(100)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := adapter.Send(ctx, sampleMessage())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected Send to fail once disconnected")
	}
	if elapsed > time.Second {
		t.Errorf("expected Send to fail fast on disconnect, took %s", elapsed)
	}
}
