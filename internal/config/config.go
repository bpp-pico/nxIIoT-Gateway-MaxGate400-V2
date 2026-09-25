package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Gateway      GatewayConfig      `yaml:"gateway"`
	API          APIConfig          `yaml:"api"`
	Database     DatabaseConfig     `yaml:"database"`
	Queue        QueueConfig        `yaml:"queue"`
	StoreForward StoreForwardConfig `yaml:"store_forward"`
	Forwarder    ForwarderConfig    `yaml:"forwarder"`
	Log          LogConfig          `yaml:"log"`
	MQTT         MQTTConfig         `yaml:"mqtt"`
	Time         TimeConfig         `yaml:"time"`
}

// StoreForwardConfig is the master switch for the whole Store & Forward
// pipeline (Rule 1's queue+forwarder half). Disabled puts the gateway into
// "Modbus read-only" mode: acquisition keeps polling and readings still
// show up live (status/logs), but processor.Process is never called (so
// nothing is queued) and the Forwarder never starts (so no
// MQTT/HTTP connection is attempted at all - see cmd/gateway/adapter.go's
// fatal-on-MQTT-connect-failure behavior, which this also sidesteps).
// Field is named Disabled (not Enabled) so the zero value - what any
// config.yaml written before this field existed unmarshals to - means
// "not disabled", i.e. existing deployments keep today's behavior with no
// extra defaulting code needed in Load.
type StoreForwardConfig struct {
	Disabled bool `yaml:"disabled"`
}

type GatewayConfig struct {
	ID   string `yaml:"id"`
	Name string `yaml:"name"`
}

type APIConfig struct {
	ListenAddr string `yaml:"listen_addr"`
	// StaticDir, if set, serves the built web/ frontend (vite build output)
	// for any request that doesn't match an /api route, on the same
	// listener/port as the REST API — avoids CORS and needs no separate
	// reverse proxy. Empty (default) disables static serving entirely.
	StaticDir string `yaml:"static_dir"`
}

type DatabaseConfig struct {
	Path string `yaml:"path"`
}

// QueueConfig controls the Store & Forward buffer (internal/queue), a
// SQLite file of its own separate from database.path. Two caps, both
// enforced by evicting the oldest chunks — sent or not — first:
//   - MaxBytes caps queue.db itself (default 1 GiB).
//   - MinFreePercent keeps that much of the queue's volume free whatever
//     else is filling it (default 10); SQLite cannot even delete once the
//     disk is completely full (MEMORY.md 2026-09-11).
//
// V1's max_rows / max_rows_sweep_interval_seconds / storage_full_percent /
// storage_sweep_interval_seconds / evict_batch_size are gone; yaml ignores
// them if an old config.yaml still has them.
type QueueConfig struct {
	// Path defaults to queue.db next to database.path.
	Path                   string  `yaml:"path"`
	MaxBytes               int64   `yaml:"max_bytes"`
	MinFreePercent         float64 `yaml:"min_free_percent"`
	FlushIntervalMs        int     `yaml:"flush_interval_ms"`
	MaintenanceIntervalSec int     `yaml:"maintenance_interval_seconds"`
}

// ForwarderConfig controls Store & Forward (§9). Transport selects which
// Adapter main.go wires up: "http" targets the dev/test adapter
// (cmd/server-demo, ServerURL); "mqtt" targets MQTTAdapter (see MQTTConfig).
type ForwarderConfig struct {
	Transport string `yaml:"transport"`
	ServerURL string `yaml:"server_url"`
	// MaxReadingsPerMessage caps one message (whole chunks are added until
	// it is reached). Replaces V1's batch_size.
	MaxReadingsPerMessage int `yaml:"max_readings_per_message"`
	PollIntervalMs        int `yaml:"poll_interval_ms"`
	SendTimeoutMs         int `yaml:"send_timeout_ms"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// MQTTConfig configures MQTTAdapter (Phase 5, §15). DataTopic/AckTopic
// default to "gateway/<client_id>/data" and "/ack" when left blank — see
// Load. TLS is only applied when TLS.Enabled is true.
type MQTTConfig struct {
	BrokerURL        string        `yaml:"broker_url"`
	ClientID         string        `yaml:"client_id"`
	Username         string        `yaml:"username"`
	Password         string        `yaml:"password"`
	QoS              int           `yaml:"qos"`
	DataTopic        string        `yaml:"data_topic"`
	AckTopic         string        `yaml:"ack_topic"`
	KeepAliveSec     int           `yaml:"keepalive_seconds"`
	ConnectTimeoutMs int           `yaml:"connect_timeout_ms"`
	PublishTimeoutMs int           `yaml:"publish_timeout_ms"`
	AckTimeoutMs     int           `yaml:"ack_timeout_ms"`
	TLS              MQTTTLSConfig `yaml:"tls"`
}

// MQTTTLSConfig is deliberately file-path based rather than embedding raw
// PEM in YAML, so the exported config JSON (§18) never has to worry about
// leaking key material — only paths, which are meaningless off the host.
type MQTTTLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// BuildMQTTTLSConfig resolves a MQTTTLSConfig's file paths into a real
// *tls.Config, nil when TLS is disabled. Shared by cmd/gateway/adapter.go
// (the production connect path) and internal/api/settings.go (the
// Settings-page live test-connect) so there's exactly one implementation
// to keep correct rather than two that can drift.
func BuildMQTTTLSConfig(cfg MQTTTLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %s contains no valid certificates", cfg.CAFile)
		}
		tlsConfig.RootCAs = pool
	}

	if cfg.CertFile != "" || cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// TimeConfig configures the Time Service (Phase 6, §11-§14). NTPServer
// empty disables NTP sync (the service still reports RTC/UNSYNCED per
// §11's fallback priority). RTCDevice is only used on Linux — see
// internal/time/rtc_linux.go.
type TimeConfig struct {
	NTPServer       string `yaml:"ntp_server"`
	Timezone        string `yaml:"timezone"`
	SyncIntervalSec int    `yaml:"sync_interval_seconds"`
	QueryTimeoutMs  int    `yaml:"query_timeout_ms"`
	RTCDevice       string `yaml:"rtc_device"`
}

// Load reads configuration from path, then applies GATEWAY_CONFIG env var
// overrides are handled by the caller; environment variable expansion in the
// YAML file itself (e.g. ${VAR}) is supported via os.ExpandEnv.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	expanded := os.ExpandEnv(string(raw))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, err
	}

	if cfg.Queue.Path == "" {
		cfg.Queue.Path = filepath.Join(filepath.Dir(cfg.Database.Path), "queue.db")
	}
	if cfg.Queue.MaxBytes <= 0 {
		// 1 GiB (user decision 2026-09-25): at the ~9 B/reading measured for
		// the chunk format that is months of buffer at the device's
		// ~12.5 readings/s, and leaves room on its 1.6 GB /userdata.
		cfg.Queue.MaxBytes = 1 << 30
	}
	if cfg.Queue.MinFreePercent <= 0 {
		cfg.Queue.MinFreePercent = 10
	}
	if cfg.Queue.FlushIntervalMs <= 0 {
		cfg.Queue.FlushIntervalMs = 2000
	}
	if cfg.Queue.MaintenanceIntervalSec <= 0 {
		cfg.Queue.MaintenanceIntervalSec = 30
	}
	if cfg.Forwarder.MaxReadingsPerMessage <= 0 {
		cfg.Forwarder.MaxReadingsPerMessage = 2000
	}
	if cfg.Forwarder.PollIntervalMs <= 0 {
		cfg.Forwarder.PollIntervalMs = 1000
	}
	if cfg.Forwarder.SendTimeoutMs <= 0 {
		cfg.Forwarder.SendTimeoutMs = 5000
	}
	if cfg.Forwarder.Transport == "" {
		cfg.Forwarder.Transport = "http"
	}

	if cfg.MQTT.QoS <= 0 {
		cfg.MQTT.QoS = 1
	}
	if cfg.MQTT.ClientID == "" {
		cfg.MQTT.ClientID = cfg.Gateway.ID
	}
	if cfg.MQTT.DataTopic == "" {
		cfg.MQTT.DataTopic = "gateway/" + cfg.MQTT.ClientID + "/data"
	}
	if cfg.MQTT.AckTopic == "" {
		cfg.MQTT.AckTopic = "gateway/" + cfg.MQTT.ClientID + "/ack"
	}
	if cfg.MQTT.KeepAliveSec <= 0 {
		cfg.MQTT.KeepAliveSec = 30
	}
	if cfg.MQTT.ConnectTimeoutMs <= 0 {
		cfg.MQTT.ConnectTimeoutMs = 5000
	}
	if cfg.MQTT.PublishTimeoutMs <= 0 {
		cfg.MQTT.PublishTimeoutMs = 5000
	}
	if cfg.MQTT.AckTimeoutMs <= 0 {
		cfg.MQTT.AckTimeoutMs = 10000
	}

	if cfg.Time.SyncIntervalSec <= 0 {
		cfg.Time.SyncIntervalSec = 300
	}
	if cfg.Time.QueryTimeoutMs <= 0 {
		cfg.Time.QueryTimeoutMs = 3000
	}
	if cfg.Time.RTCDevice == "" {
		cfg.Time.RTCDevice = "/dev/rtc0"
	}
	if cfg.Time.Timezone == "" {
		cfg.Time.Timezone = "UTC"
	}

	return &cfg, nil
}

// Save writes cfg back to path as YAML, replacing its previous contents.
// Used by the Web UI's Settings save (§16/§18): the Gateway/MQTT/Time
// settings the doc describes as configured via config.yaml can now be
// edited from the browser instead of by hand.
//
// This is a full re-marshal of the struct, not a targeted patch, so any
// hand-written comments/formatting in the existing file are lost once a
// save happens through the API. That's a deliberate simplicity tradeoff
// (a comment-preserving YAML patcher is significantly more code) — the
// file remains fully valid, readable YAML, just without the original
// inline comments.
func Save(path string, cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}
