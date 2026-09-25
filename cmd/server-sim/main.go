// server-sim is a minimal fake "Internal Server" used only for local
// development and testing of the Store & Forward engine (internal/forwarder).
// It is not part of the gateway binary and is never deployed to production.
//
// It is the reference for the server side of the V2 wire contract
// (Server_Design_Spec.md): at-least-once delivery + idempotent processing.
// It de-duplicates whole chunks on (gateway_id, stream_id, seq), so a
// message resent after a lost ack does not double-count, and it logs a gap
// when seq jumps within a stream (the gateway evicted data when full).
//
// It speaks both transports behind forwarder.Adapter (§15): HTTP (/ingest,
// for HTTPAdapter) and, when -mqtt-broker is set, MQTT (for MQTTAdapter) —
// subscribing to the data topic and publishing an application-level ack
// back per message. Both share the same in-memory store.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"nxiiot-gateway/internal/forwarder"
)

// reading is one stored value, flattened for /received.
type reading struct {
	GatewayID string `json:"gateway_id"`
	StreamID  string `json:"stream_id"`
	Seq       int64  `json:"seq"`
	forwarder.WireReading
}

type chunkKey struct {
	gatewayID, streamID string
	seq                 int64
}

type streamKey struct{ gatewayID, streamID string }

type store struct {
	mu       sync.Mutex
	seen     map[chunkKey]bool
	lastSeq  map[streamKey]int64
	received []reading
}

func newStore() *store {
	return &store{seen: make(map[chunkKey]bool), lastSeq: make(map[streamKey]int64)}
}

// ingest stores every chunk of m not seen before and reports how many
// readings were new and how many chunks were duplicates.
func (s *store) ingest(m forwarder.Message) (accepted, duplicateChunks int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sk := streamKey{m.GatewayID, m.StreamID}
	for _, c := range m.Chunks {
		k := chunkKey{m.GatewayID, m.StreamID, c.Seq}
		if s.seen[k] {
			duplicateChunks++
			continue
		}
		s.seen[k] = true
		if last, ok := s.lastSeq[sk]; ok && c.Seq > last+1 {
			log.Printf("gap: gateway=%s stream=%s seq %d-%d missing (evicted on the gateway)", m.GatewayID, m.StreamID, last+1, c.Seq-1)
		}
		if c.Seq > s.lastSeq[sk] {
			s.lastSeq[sk] = c.Seq
		}
		for _, r := range c.Readings {
			s.received = append(s.received, reading{GatewayID: m.GatewayID, StreamID: m.StreamID, Seq: c.Seq, WireReading: r})
			accepted++
		}
	}
	return accepted, duplicateChunks
}

func (s *store) all() []reading {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]reading, len(s.received))
	copy(out, s.received)
	return out
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	mqttBroker := flag.String("mqtt-broker", "", "MQTT broker URL (e.g. tcp://mosquitto:1883); empty disables the MQTT consumer")
	mqttDataTopic := flag.String("mqtt-data-topic", "gateway/+/data", "MQTT topic filter to subscribe to for incoming messages")
	flag.Parse()

	if v := os.Getenv("MQTT_BROKER_URL"); v != "" {
		*mqttBroker = v
	}

	st := newStore()

	if *mqttBroker != "" {
		startMQTTConsumer(*mqttBroker, *mqttDataTopic, st)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var m forwarder.Message
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		accepted, duplicates := st.ingest(m)
		log.Printf("ingest: gateway=%s seq=%d-%d readings=%d accepted=%d duplicate_chunks=%d", m.GatewayID, m.FromSeq, m.ToSeq, m.ReadingCount(), accepted, duplicates)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"accepted": accepted, "duplicates": duplicates})
	})
	mux.HandleFunc("/received", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(st.all())
	})

	log.Printf("server-sim listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

// startMQTTConsumer subscribes to dataTopicFilter (e.g. "gateway/+/data")
// and, for every message received, dedupes it into st and publishes an
// application-level ack (§15) back to the sender's ack topic — derived by
// swapping the topic's "/data" suffix for "/ack", matching the default
// topic scheme MQTTAdapter derives in config.Load.
func startMQTTConsumer(broker, dataTopicFilter string, st *store) {
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("server-sim").
		SetAutoReconnect(true).
		SetConnectRetry(true)

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Printf("mqtt: connected to %s, subscribing to %s", broker, dataTopicFilter)
		token := c.Subscribe(dataTopicFilter, 1, func(client mqtt.Client, msg mqtt.Message) {
			var m forwarder.Message
			if err := json.Unmarshal(msg.Payload(), &m); err != nil {
				log.Printf("mqtt: invalid message payload on %s: %v", msg.Topic(), err)
				return
			}

			accepted, duplicates := st.ingest(m)
			log.Printf("mqtt ingest: topic=%s seq=%d-%d readings=%d accepted=%d duplicate_chunks=%d",
				msg.Topic(), m.FromSeq, m.ToSeq, m.ReadingCount(), accepted, duplicates)

			// Ack only after the data is stored; echo to_seq (and stream_id).
			ackTopic := strings.TrimSuffix(msg.Topic(), "/data") + "/ack"
			ack, _ := json.Marshal(forwarder.Ack{StreamID: m.StreamID, ToSeq: m.ToSeq})
			client.Publish(ackTopic, 1, false, ack)
		})
		token.Wait()
		if token.Error() != nil {
			log.Printf("mqtt: subscribe failed: %v", token.Error())
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("mqtt: connection lost, will auto-reconnect: %v", err)
	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		log.Fatalf("mqtt: initial connect to %s failed: %v", broker, token.Error())
	}
}
