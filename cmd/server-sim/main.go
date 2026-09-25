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
// for HTTPAdapter) and MQTT (for MQTTAdapter) — subscribing to the data
// topic and publishing an application-level ack back per message.
//
// When INFLUX_URL is set, new chunks are written to InfluxDB 2.x
// (influx.go) before the message is acked; if that write fails the message
// is rejected (HTTP 503, or an MQTT ack with "error"), so the gateway keeps
// the data and resends it.
//
// The page at / shows per-gateway status and edits Settings (settings.go):
// simulated faults, the gateway registry, datapoint names, the MQTT broker
// and topic, and the InfluxDB bucket and retention. Settings are saved to
// SETTINGS_PATH when it is set. The page has no login: test use only.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	mqttBroker := flag.String("mqtt-broker", "", "initial MQTT broker URL (e.g. tcp://emqx:1883); empty disables the MQTT consumer")
	mqttDataTopic := flag.String("mqtt-data-topic", "gateway/+/data", "initial MQTT topic filter for incoming messages")
	flag.Parse()

	// Environment and flags only seed the first start; after that the
	// settings file (edited from the page) wins.
	defaults := Settings{
		Fault:  FaultSettings{Mode: faultNone},
		MQTT:   MQTTSettings{BrokerURL: envOr("MQTT_BROKER_URL", *mqttBroker), DataTopic: *mqttDataTopic},
		Influx: InfluxSettings{Bucket: envOr("INFLUX_BUCKET", "gateway"), RetentionDays: 30},
	}
	if v, err := strconv.Atoi(os.Getenv("INFLUX_RETENTION_DAYS")); err == nil {
		defaults.Influx.RetentionDays = v
	}
	settings, err := loadSettings(os.Getenv("SETTINGS_PATH"), defaults)
	if err != nil {
		log.Fatalf("settings: %v", err)
	}

	sv := &server{settings: settings, startedAt: time.Now().UTC()}
	ctx := context.Background()
	if u := os.Getenv("INFLUX_URL"); u != "" {
		sv.influx = newInfluxWriter(u, os.Getenv("INFLUX_ORG"), os.Getenv("INFLUX_TOKEN"), func() string { return sv.settings.get().Influx.Bucket })
		set := settings.get()
		if err := sv.influx.ensureBucket(ctx, set.Influx.Bucket, set.Influx.RetentionDays); err != nil {
			log.Printf("InfluxDB: could not check bucket %q: %v", set.Influx.Bucket, err)
		}
		log.Printf("writing readings to InfluxDB at %s (org=%s bucket=%s)", u, sv.influx.org, set.Influx.Bucket)
		go sv.refreshMeta(ctx, 6*time.Hour)
		sv.store = newStore(sv.influx)
	} else {
		sv.store = newStore(nil)
	}

	sv.mqtt = &mqttConsumer{handle: sv.handleMQTT}
	set := settings.get()
	sv.mqtt.apply(set.MQTT.BrokerURL, set.MQTT.DataTopic)

	log.Printf("server-sim listening on %s", *addr)
	if err := http.ListenAndServe(*addr, sv.routes()); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
