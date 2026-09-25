package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"nxiiot-gateway/internal/config"
	"nxiiot-gateway/internal/forwarder"
)

// settingsDTO covers the config.yaml fields the user asked to edit from
// the Web UI (Gateway identity, MQTT broker, Time/NTP, queue max rows) —
// everything else in config.yaml (database path, other queue thresholds,
// forwarder batch tuning, log level) stays file-only, no UI surface for it.
type settingsDTO struct {
	Gateway struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"gateway"`
	MQTT struct {
		// Transport selects forwarder.transport ("http" or "mqtt" - see
		// config.ForwarderConfig). Exposed here, not as its own section,
		// since it's meaningless without the MQTT fields alongside it in
		// the same form.
		Transport string `json:"transport"`
		BrokerURL string `json:"broker_url"`
		ClientID  string `json:"client_id"`
		Username  string `json:"username,omitempty"`
		// Password is never sent to the browser (§18) and left unchanged
		// unless a new value is submitted here.
		Password     string `json:"password,omitempty"`
		QoS          int    `json:"qos"`
		DataTopic    string `json:"data_topic,omitempty"`
		AckTopic     string `json:"ack_topic,omitempty"`
		KeepAliveSec int    `json:"keepalive_seconds"`
	} `json:"mqtt"`
	Time struct {
		NTPServer       string `json:"ntp_server"`
		Timezone        string `json:"timezone"`
		SyncIntervalSec int    `json:"sync_interval_seconds"`
	} `json:"time"`
	Queue struct {
		// MaxBytes caps queue.db; MinFreePercent is the free space kept on
		// its volume. Either limit evicts the oldest queued data.
		MaxBytes       int64   `json:"max_bytes"`
		MinFreePercent float64 `json:"min_free_percent"`
	} `json:"queue"`
	// StoreForward is the master switch for the whole pipeline (see
	// config.StoreForwardConfig) - when Enabled is false, the gateway only
	// polls Modbus; nothing is queued or sent anywhere.
	StoreForward struct {
		Enabled bool `json:"enabled"`
	} `json:"store_forward"`
}

// normalizeNTPServer strips a leading /etc/ntp.conf-style "server "/"pool "
// token (a common paste mistake — the Web UI field wants a bare
// hostname/IP, not a config-file line) and rejects anything that still
// contains whitespace afterward, since that can never resolve as a
// hostname and would otherwise fail silently at DNS-lookup time with no
// error surfaced anywhere in the UI (see MEMORY.md's 2026-08-27 entry).
// An empty string is valid and disables NTP sync.
func normalizeNTPServer(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", nil
	}
	for _, prefix := range []string{"server ", "pool "} {
		if strings.HasPrefix(strings.ToLower(s), prefix) {
			s = strings.TrimSpace(s[len(prefix):])
			break
		}
	}
	if strings.ContainsAny(s, " \t\r\n") {
		return "", fmt.Errorf("ntp_server must be a bare hostname or IP address, not an ntp.conf-style line (got %q)", raw)
	}
	return s, nil
}

// normalizeBrokerURL trims whitespace and confirms broker_url will actually
// parse into a usable paho server entry before it's ever written to
// config.yaml. A stray leading/trailing space (e.g. left over from an
// earlier placeholder value that a later edit appended to instead of
// replacing) makes net/url.Parse fail; paho's AddBroker swallows that
// parse error into its own internal (silent-by-default) logger and simply
// adds nothing to its server list, so Connect() later fails with "no
// servers defined to connect to" - and since a failed initial MQTT
// connect is fatal at gateway startup (cmd/gateway/adapter.go), that
// silently-malformed value crash-loops the whole process, not just
// forwarding, with no error ever surfaced anywhere in the UI (see
// MEMORY.md's 2026-09-03 entry - this exact scenario happened for real).
func normalizeBrokerURL(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", fmt.Errorf("mqtt.broker_url is required")
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("mqtt.broker_url must be a valid broker URL like tcp://host:1883 (got %q)", raw)
	}
	return s, nil
}

// testMQTTConnect attempts a real, short-lived connect to the broker about
// to be saved, using a client ID distinct from the one about to go live -
// never the same ID, since brokers commonly enforce "last connection
// wins" per client ID and a same-ID test would briefly kick the currently
// running production connection. Returns nil immediately for anything
// other than a StoreForward-enabled, mqtt-transport save (an http save,
// or a disabled Store & Forward, never attempts a broker connection at
// startup either - see main.go/adapter.go - so there's nothing to test).
//
// This exists because normalizeBrokerURL only catches a value that can't
// even parse as a URL - a syntactically valid but wrong or unreachable
// broker (typo'd host, broker not actually listening, bad credentials)
// sails through that check, gets saved, and fails at the *next* restart
// exactly like the incident normalizeBrokerURL was built for: fatal at
// startup (cmd/gateway/adapter.go), crash-looping the whole gateway
// including the web UI used to fix it. See MEMORY.md's 2026-09-03/04
// entries for the real incident and the deliberate decision (after
// experiencing that cost) to add this live check rather than rely on
// syntax validation alone.
func (s *Server) testMQTTConnect(cfg config.MQTTConfig, transport string) error {
	if transport != "mqtt" {
		return nil
	}

	tlsConfig, err := config.BuildMQTTTLSConfig(cfg.TLS)
	if err != nil {
		return fmt.Errorf("tls config: %w", err)
	}

	timeout := time.Duration(cfg.ConnectTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	testClientID := cfg.ClientID
	if testClientID == "" {
		testClientID = "gateway"
	}
	testClientID = fmt.Sprintf("%s-settings-test-%d", testClientID, time.Now().UnixNano())

	adapter := forwarder.NewMQTTAdapter(forwarder.MQTTAdapterConfig{
		BrokerURL:      cfg.BrokerURL,
		ClientID:       testClientID,
		Username:       cfg.Username,
		Password:       cfg.Password,
		QoS:            byte(cfg.QoS),
		DataTopic:      cfg.DataTopic,
		AckTopic:       cfg.AckTopic,
		KeepAlive:      time.Duration(cfg.KeepAliveSec) * time.Second,
		ConnectTimeout: timeout,
		TLS:            tlsConfig,
	}, s.log)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := adapter.Connect(ctx); err != nil {
		return fmt.Errorf("could not connect to MQTT broker %s: %w", cfg.BrokerURL, err)
	}
	adapter.Disconnect(250)
	return nil
}

func (s *Server) getSettings(w http.ResponseWriter, r *http.Request) {
	var dto settingsDTO
	dto.Gateway.ID = s.cfg.Gateway.ID
	dto.Gateway.Name = s.cfg.Gateway.Name
	dto.MQTT.Transport = s.cfg.Forwarder.Transport
	dto.MQTT.BrokerURL = s.cfg.MQTT.BrokerURL
	dto.MQTT.ClientID = s.cfg.MQTT.ClientID
	dto.MQTT.Username = s.cfg.MQTT.Username
	dto.MQTT.QoS = s.cfg.MQTT.QoS
	dto.MQTT.DataTopic = s.cfg.MQTT.DataTopic
	dto.MQTT.AckTopic = s.cfg.MQTT.AckTopic
	dto.MQTT.KeepAliveSec = s.cfg.MQTT.KeepAliveSec
	dto.Time.NTPServer = s.cfg.Time.NTPServer
	dto.Time.Timezone = s.cfg.Time.Timezone
	dto.Time.SyncIntervalSec = s.cfg.Time.SyncIntervalSec
	dto.Queue.MaxBytes = s.cfg.Queue.MaxBytes
	dto.Queue.MinFreePercent = s.cfg.Queue.MinFreePercent
	dto.StoreForward.Enabled = !s.cfg.StoreForward.Disabled
	writeJSON(w, http.StatusOK, dto)
}

// saveSettings writes the submitted Gateway/MQTT/Time fields into
// config.yaml and restarts the process so they take effect — the "apply
// behavior" the user chose over live-reloading the MQTT client and Time
// Service in place. In this dev docker-compose stack, air's file watcher
// (which already watches configs/) restarts the rebuilt binary on its
// own; os.Exit here additionally makes this work under any supervisor
// that restarts on exit (systemd Restart=always, Docker's
// restart:unless-stopped), which is what a non-dev deployment actually
// runs under.
func (s *Server) saveSettings(w http.ResponseWriter, r *http.Request) {
	var dto settingsDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if dto.Gateway.ID == "" {
		writeError(w, http.StatusBadRequest, "gateway.id is required")
		return
	}
	// A disabled Store & Forward never builds an adapter (see main.go), so
	// the transport/broker fields are moot - skip validating them. Validate
	// broker_url even when transport is "http" (not just "mqtt") - a bad
	// value left behind an inert http transport is exactly what crash-
	// looped the gateway for real once the transport was later switched
	// (or just re-saved) with mqtt selected; see normalizeBrokerURL.
	var brokerURL string
	if dto.StoreForward.Enabled {
		var err error
		brokerURL, err = normalizeBrokerURL(dto.MQTT.BrokerURL)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		switch dto.MQTT.Transport {
		case "http", "mqtt":
		default:
			writeError(w, http.StatusBadRequest, `transport must be "http" or "mqtt"`)
			return
		}
	}

	// Build the effective MQTT config this save would produce without
	// mutating s.cfg yet, so it can be live-tested first (below) and only
	// actually committed if the test passes - never leaving s.cfg/disk in
	// a half-applied state on a rejected save.
	newMQTT := s.cfg.MQTT
	if dto.StoreForward.Enabled {
		newMQTT.BrokerURL = brokerURL
	} else {
		// Not validated above (moot while disabled) - still trim so a
		// stray space doesn't linger in config.yaml waiting to bite if
		// Store & Forward is re-enabled later without touching this field.
		newMQTT.BrokerURL = strings.TrimSpace(dto.MQTT.BrokerURL)
	}
	newMQTT.ClientID = dto.MQTT.ClientID
	newMQTT.Username = dto.MQTT.Username
	if dto.MQTT.Password != "" {
		newMQTT.Password = dto.MQTT.Password
	}
	if dto.MQTT.QoS > 0 {
		newMQTT.QoS = dto.MQTT.QoS
	}
	if dto.MQTT.DataTopic != "" {
		newMQTT.DataTopic = dto.MQTT.DataTopic
	}
	if dto.MQTT.AckTopic != "" {
		newMQTT.AckTopic = dto.MQTT.AckTopic
	}
	if dto.MQTT.KeepAliveSec > 0 {
		newMQTT.KeepAliveSec = dto.MQTT.KeepAliveSec
	}

	if dto.StoreForward.Enabled {
		if err := s.testMQTTConnect(newMQTT, dto.MQTT.Transport); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	s.cfg.Gateway.ID = dto.Gateway.ID
	s.cfg.Gateway.Name = dto.Gateway.Name
	s.cfg.StoreForward.Disabled = !dto.StoreForward.Enabled
	s.cfg.Forwarder.Transport = dto.MQTT.Transport
	s.cfg.MQTT = newMQTT
	ntpServer, err := normalizeNTPServer(dto.Time.NTPServer)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.cfg.Time.NTPServer = ntpServer
	s.cfg.Time.Timezone = dto.Time.Timezone
	if dto.Time.SyncIntervalSec > 0 {
		s.cfg.Time.SyncIntervalSec = dto.Time.SyncIntervalSec
	}
	if dto.Queue.MaxBytes > 0 {
		s.cfg.Queue.MaxBytes = dto.Queue.MaxBytes
	}
	if dto.Queue.MinFreePercent > 0 {
		s.cfg.Queue.MinFreePercent = dto.Queue.MinFreePercent
	}

	if s.configPath == "" {
		writeError(w, http.StatusInternalServerError, "config file path is unknown; cannot save")
		return
	}
	if err := config.Save(s.configPath, s.cfg); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to save config: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"saved": true, "restarting": true})

	s.log.Warn("settings saved via Web UI, restarting gateway to apply")
	go func() {
		time.Sleep(300 * time.Millisecond) // let the HTTP response flush before the process exits
		os.Exit(0)
	}()
}
