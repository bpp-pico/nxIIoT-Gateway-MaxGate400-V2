# V2 test server (Docker Desktop)

A local stand-in for the backend, for testing gateway V2 end to end. It is for dev/test only and is not a production server. None of the pages below has TLS, and the server-demo page has no login.

| Service | What it does | Open |
|---|---|---|
| `server-demo` | Speaks the V2 wire contract ([Server_Design_Spec.md](../Server_Design_Spec.md)), stores readings in InfluxDB and **acks only after the write succeeds**. Its page shows status and holds the config (see below). | http://localhost:9000 (no login) |
| `grafana` | Dashboard "nxIIoT Gateway data" (provisioned, the home page). | http://localhost:3000 (`admin` / `GRAFANA_PASSWORD`) |
| `influxdb` | InfluxDB 2.7. Org `nxiiot`, bucket `gateway`. | http://localhost:8086 (`admin` / `INFLUX_PASSWORD`) |
| `emqx` | MQTT broker (EMQX 5.8). Anonymous access, no TLS. MQTT is on port 1883. | http://localhost:18083 (`admin` / `EMQX_DASHBOARD_PASSWORD`) |

The passwords and the InfluxDB token are in `.env`, which is not committed.

## Start / stop

```sh
cd test-server
cp .env.example .env        # then replace every value (passwords, token)
docker compose up -d --build
docker compose logs -f server-demo
docker compose down         # keeps data; `down -v` also deletes all volumes (data, settings, dashboards edited in the UI)
```

- **After changing `cmd/server-demo`:** run `docker compose up -d --build server-demo`.
- **After changing the dashboard JSON:** run `docker compose restart grafana`.

## server-demo page (http://localhost:9000)

- **Status:** one row per gateway, showing:
  - last seen, messages, and readings stored;
  - duplicate chunks: resends that were de-duplicated;
  - gaps and missing chunks: data the gateway evicted;
  - rejected messages, the last `seq` per stream, and the last error.

  Recent gaps are listed below the table. These counters live in memory and reset when server-demo restarts.
- **Fault Simulation:** makes the server misbehave on purpose, for every gateway and both transports, from the next message. A banner shows while a fault is active.
  - *Lose the ack:* the message is stored, but no ack is sent. The gateway resends it, and the Duplicate chunks count should rise with no gap.
  - *Reject:* nothing is stored, and the ack carries an error. The gateway keeps the data and retries with backoff.
  - *Server down:* nothing is stored and no ack is sent. The gateway times out and resends.
  - *Ack delay:* the message is stored, then the ack waits. If the delay is longer than the gateway's `ack_timeout_ms` (10 s), you get resends.
- **Gateways:** the registry of IDs and display names. Tick *Reject unregistered gateways* to answer unknown gateways with an error ack, so they keep their data queued.
- **Datapoints:** sets the name and unit per `(gateway, device, datapoint)`. Every datapoint seen since start is listed. Grafana uses these as series labels.
- **Broker & Database:**
  - the MQTT broker URL and data topic, which reconnect or resubscribe on save;
  - the InfluxDB bucket, created if missing;
  - the retention in days, applied to the bucket.

Settings are saved to `/data/settings.json` on the `server-data` volume and survive restarts. The environment in `compose.yml` only sets the first-start defaults.

## Pointing a gateway at it

- **Gateway on this PC:** set `forwarder.transport: "mqtt"` and `mqtt.broker_url: "tcp://localhost:1883"`. Run it from the repo root, because it reads `./migrations`.
- **HTTP transport instead:** set `forwarder.server_url: "http://localhost:9000/ingest"`.
- **MaxGate400:** use `tcp://<this PC's LAN or Tailscale address>:1883`. This needs three things:
  - the device running V2, since V1 sends the old `entries` format, which server-demo rejects;
  - Windows Firewall allowing inbound TCP 1883;
  - a decision about exposure: anonymous EMQX, and the server-demo page with no login, become reachable by anything on that network.

## Data in InfluxDB

| Measurement | Tags | Fields | Time |
|---|---|---|---|
| `reading` | `gateway_id`, `device_id`, `datapoint_id` | `value` (float; left out when the read failed), `quality`, `seq`, `stream_id` | the reading's own `ts` (ms) |
| `gap` | `gateway_id` | `from_seq`, `to_seq`, `chunks`, `stream_id` | when the gap was detected |
| `datapoint_meta` | `gateway_id`, `device_id`, `datapoint_id` | `name`, `unit` | rewritten on every save and every 6 h, so retention never expires it |

- **Resends are safe:** writing the same chunk twice overwrites the same points, so a resend never double-counts.
- **Grafana variables:** the dashboard has a *Bucket* selector (for the case where you change the bucket on the server-demo page) and a *Gateway* selector.

## Limits

- server-demo remembers which chunks it has seen and each stream's last `seq` only in memory. After it restarts, a resent chunk is written to InfluxDB again. That is harmless, because the write is idempotent. The first chunk it sees after a restart cannot be checked for a gap.
- `/received` returns only the most recent 10,000 readings.
- The *Readings per minute* panel counts by reading time. A backlog sent late fills in the minutes it was recorded, not the minutes it arrived.
