package storage_test

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"nxiiot-gateway/internal/storage"
)

// TestMigration0008DropsQueueAndPriorityAgainstSeededData applies 0008 to a
// database shaped like a deployed V1 gateway (0001-0007 with real
// connection/device/datapoint rows and data_queue rows) and checks that
// data_queue and datapoint.priority are gone while every other datapoint
// column survives.
func TestMigration0008DropsQueueAndPriorityAgainstSeededData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	v1 := []string{
		"0001_init.sql",
		"0002_device_rtu_params.sql",
		"0003_data_queue_retry.sql",
		"0004_connection_split.sql",
		"0005_scan_polling.sql",
		"0006_data_queue_event_timestamp_index.sql",
		"0007_data_queue_priority_eviction_index.sql",
	}

	preDir := t.TempDir()
	for _, m := range v1 {
		copyMigration(t, preDir, m)
	}
	db, err := storage.Open(dbPath, preDir, log)
	if err != nil {
		t.Fatalf("open V1 schema: %v", err)
	}
	mustExec(t, db, `INSERT INTO connection (id, name, protocol, interface, baud_rate, data_bits, parity, stop_bits, timeout_ms, retry, enabled)
		VALUES (1, 'A1B1', 'RTU', '/dev/ttyS1', 9600, 8, 'N', 1, 1000, 3, 1)`)
	mustExec(t, db, `INSERT INTO device (id, name, connection_id, slave_id, enabled) VALUES (1, 'MD-XY02-1', 1, 1, 1)`)
	mustExec(t, db, `INSERT INTO datapoint (id, device_id, tag_name, function_code, register_address, data_type, scale, unit, priority, enabled)
		VALUES (1, 1, 'temperature', 4, 1, 'INT16', 0.1, 'C', 'CRITICAL', 1)`)
	mustExec(t, db, `INSERT INTO gateway (id, name) VALUES ('GW002', 'MaxGate400')`)
	mustExec(t, db, `INSERT INTO data_queue (gateway_id, sequence_id, device_id, datapoint_id, value, quality, event_timestamp, status, priority)
		VALUES ('GW002', 1, 1, 1, 28.4, 'GOOD', '2026-09-11T12:00:00.000Z', 'SENT', 'CRITICAL')`)
	if err := db.Close(); err != nil {
		t.Fatalf("close V1 db: %v", err)
	}

	postDir := t.TempDir()
	for _, m := range append(v1, "0008_v2_drop_data_queue_and_priority.sql") {
		copyMigration(t, postDir, m)
	}
	db, err = storage.Open(dbPath, postDir, log)
	if err != nil {
		t.Fatalf("apply 0008 against seeded db: %v", err)
	}
	defer db.Close()

	var tables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%data_queue%'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Errorf("data_queue or one of its indexes still exists (%d objects)", tables)
	}
	assertColumnDropped(t, db, "datapoint", "priority")

	var tag, unit string
	var scale float64
	if err := db.QueryRow(`SELECT tag_name, scale, unit FROM datapoint WHERE id = 1`).Scan(&tag, &scale, &unit); err != nil {
		t.Fatalf("query datapoint: %v", err)
	}
	if tag != "temperature" || scale != 0.1 || unit != "C" {
		t.Errorf("datapoint row changed: %q %v %q", tag, scale, unit)
	}
	var gw string
	if err := db.QueryRow(`SELECT id FROM gateway`).Scan(&gw); err != nil || gw != "GW002" {
		t.Errorf("gateway row lost: %q %v", gw, err)
	}
}
