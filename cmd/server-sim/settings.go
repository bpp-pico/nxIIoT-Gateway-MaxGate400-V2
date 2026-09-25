package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Fault modes let a tester make the server misbehave on purpose, to check
// that the gateway keeps its data and resends it correctly.
const (
	faultNone     = "none"      // normal: store, then ack
	faultDropAck  = "drop_ack"  // store, but never ack (a lost ack): the resend must be de-duplicated
	faultErrorAck = "error_ack" // store nothing, ack with "error": the gateway must resend
	faultIgnore   = "ignore"    // store nothing, no ack (server down): the gateway must resend
)

// Settings is everything editable on the config page. It is saved as JSON
// at the settings path so it survives a restart of the container.
type Settings struct {
	Fault                 FaultSettings   `json:"fault"`
	RejectUnknownGateways bool            `json:"reject_unknown_gateways"`
	Gateways              []GatewayEntry  `json:"gateways"`
	Datapoints            []DatapointName `json:"datapoints"`
	MQTT                  MQTTSettings    `json:"mqtt"`
	Influx                InfluxSettings  `json:"influx"`
}

type FaultSettings struct {
	Mode       string `json:"mode"`
	AckDelayMs int    `json:"ack_delay_ms"` // wait this long before acking stored data
}

type GatewayEntry struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type DatapointName struct {
	GatewayID   string `json:"gateway_id"`
	DeviceID    int64  `json:"device_id"`
	DatapointID int64  `json:"datapoint_id"`
	Name        string `json:"name"`
	Unit        string `json:"unit"`
}

type MQTTSettings struct {
	BrokerURL string `json:"broker_url"` // empty disables the MQTT consumer
	DataTopic string `json:"data_topic"` // must end in "/data"; the ack goes to the same topic with "/ack"
}

type InfluxSettings struct {
	Bucket        string `json:"bucket"`
	RetentionDays int    `json:"retention_days"` // 0 = keep forever
}

func (s Settings) validate() error {
	switch s.Fault.Mode {
	case faultNone, faultDropAck, faultErrorAck, faultIgnore:
	default:
		return fmt.Errorf("fault.mode must be one of none, drop_ack, error_ack, ignore")
	}
	if s.Fault.AckDelayMs < 0 || s.Fault.AckDelayMs > 120000 {
		return errors.New("fault.ack_delay_ms must be 0-120000")
	}
	seen := map[string]bool{}
	for _, g := range s.Gateways {
		if strings.TrimSpace(g.ID) == "" {
			return errors.New("every gateway needs an id")
		}
		if seen[g.ID] {
			return fmt.Errorf("gateway %q is listed twice", g.ID)
		}
		seen[g.ID] = true
	}
	if s.MQTT.BrokerURL != "" {
		u, err := url.Parse(s.MQTT.BrokerURL)
		if err != nil || u.Host == "" || u.Scheme == "" {
			return fmt.Errorf("mqtt.broker_url %q is not a URL like tcp://host:1883", s.MQTT.BrokerURL)
		}
	}
	if !strings.HasSuffix(s.MQTT.DataTopic, "/data") {
		return errors.New(`mqtt.data_topic must end in "/data" (e.g. gateway/+/data)`)
	}
	if strings.TrimSpace(s.Influx.Bucket) == "" || strings.HasPrefix(s.Influx.Bucket, "_") {
		return errors.New("influx.bucket must be set and must not start with _")
	}
	if s.Influx.RetentionDays < 0 {
		return errors.New("influx.retention_days must be 0 (forever) or more")
	}
	return nil
}

func (s *Settings) normalize() {
	s.Fault.Mode = strings.TrimSpace(s.Fault.Mode)
	if s.Fault.Mode == "" {
		s.Fault.Mode = faultNone
	}
	for i := range s.Gateways {
		s.Gateways[i].ID = strings.TrimSpace(s.Gateways[i].ID)
		s.Gateways[i].Name = strings.TrimSpace(s.Gateways[i].Name)
	}
	// A datapoint with neither a name nor a unit is just "not named".
	dps := s.Datapoints[:0]
	for _, d := range s.Datapoints {
		d.Name, d.Unit = strings.TrimSpace(d.Name), strings.TrimSpace(d.Unit)
		if d.Name != "" || d.Unit != "" {
			dps = append(dps, d)
		}
	}
	s.Datapoints = dps
	s.MQTT.BrokerURL = strings.TrimSpace(s.MQTT.BrokerURL)
	s.MQTT.DataTopic = strings.TrimSpace(s.MQTT.DataTopic)
	s.Influx.Bucket = strings.TrimSpace(s.Influx.Bucket)
}

// settingsStore holds the current Settings and persists every change.
type settingsStore struct {
	mu   sync.RWMutex
	path string // empty: memory only
	cur  Settings
}

// loadSettings reads path if it exists, otherwise starts from defaults.
func loadSettings(path string, defaults Settings) (*settingsStore, error) {
	st := &settingsStore{path: path, cur: defaults}
	if path == "" {
		return st, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return st, st.save(defaults)
	}
	if err != nil {
		return nil, err
	}
	s := defaults
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	s.normalize()
	if err := s.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	st.cur = s
	return st, nil
}

func (st *settingsStore) get() Settings {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.cur
}

// set validates s, writes it to disk, and makes it current.
func (st *settingsStore) set(s Settings) (Settings, error) {
	s.normalize()
	if err := s.validate(); err != nil {
		return Settings{}, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.save(s); err != nil {
		return Settings{}, err
	}
	st.cur = s
	return s, nil
}

func (st *settingsStore) save(s Settings) error {
	if st.path == "" {
		return nil
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(st.path), 0o755); err != nil {
		return err
	}
	tmp := st.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, st.path)
}

func (s Settings) gatewayName(id string) (name string, registered bool) {
	for _, g := range s.Gateways {
		if g.ID == id {
			return g.Name, true
		}
	}
	return "", false
}
