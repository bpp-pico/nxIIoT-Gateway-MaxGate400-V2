# nxIIoT Gateway

> **V2 fork** of `nxIIoT-Gateway-MaxGate400` (V1), forked at commit `58cde8d`. Everything below still describes V1 until V2's scope is defined — see the note at the top of [spec.md](spec.md).

Industrial IoT Gateway for Modbus RTU/TCP acquisition, local store & forward, and MQTT delivery to a downstream server. This repo is the gateway component only (Go backend) — split out from the original monorepo for deployment to a maisvch MaxGate400 device. See [industrial_iot_gateway_handoff_dev_plan.md](industrial_iot_gateway_handoff_dev_plan.md) if present in your checkout for the full design and phased development plan, and [HANDOFF.md](HANDOFF.md) for the current orientation snapshot (architecture decisions, known gaps, what's next).

## Stack

- Backend: Go
- Database: SQLite (WAL mode)
- Protocols: Modbus RTU/TCP, MQTT (QoS 1 + application-level ack)
- Time: hand-rolled SNTP client, Linux hardware RTC support

## Development

Native build (no Docker required):

```bash
go build -o gateway ./cmd/gateway
./gateway -config configs/config.yaml -migrations migrations
```

- Backend API: http://localhost:8080/api/system

Two dev-only simulators live under `cmd/` purely to make the gateway testable without physical hardware or a real downstream server — never deployed to production:
- `cmd/modbus-sim` — fake Modbus TCP slave
- `cmd/server-sim` — fake HTTP/MQTT consumer for the V2 contract that dedupes on `(gateway_id, stream_id, seq)` and optionally writes to InfluxDB; `test-server/` runs it with EMQX + InfluxDB under Docker (see [test-server/README.md](test-server/README.md))

With `SEED_DEMO_DEVICE=true`, the gateway seeds one demo Modbus TCP device (`PM001`, pointed at `modbus-sim`) on first boot if the `device` table is empty.

Every acquired reading is persisted to `data_queue` and then forwarded in batches. Check `GET /api/store-forward/status` for pending/sending counts, retry count, disk usage %/level, and server connectivity.

For RTU devices, the "Serial Interface" field (via the admin API/UI) shows a dropdown of ports detected on the gateway host (`GET /api/system/serial-ports`), falling back to manual entry (e.g. `/dev/ttyUSB0`) if none are detected.

### Saving settings

Settings changes (Gateway/MQTT/Time) write straight to `configs/config.yaml` and restart the gateway process to apply — a few seconds of downtime. This rewrites the whole file, so **any hand-edited comments in `config.yaml` are lost the first time a save happens through the API/UI**.

Setting the gateway host's actual network interface IP (via NetworkManager's `nmcli`) is only meaningful when the binary runs directly on a Linux host with NetworkManager present. Applying a static IP is protected by a 45-second confirm-or-auto-revert window (`internal/netconfig`) — a typo'd address self-heals instead of locking you out.

### Testing RTU against real hardware on Windows

USB-to-RS485 adapters attach as a native Windows COM port. Build and run the gateway natively to test against a real port:

```powershell
go build -o gateway.exe ./cmd/gateway
./gateway.exe -config configs/config.yaml -migrations migrations
```

## Project layout

```
cmd/            gateway, modbus-sim, server-sim entrypoints
internal/       acquisition, connection, device, datapoint, queue, forwarder, time, netconfig, api, storage, ...
migrations/     SQLite schema migrations
configs/        config.yaml (dev defaults), mosquitto.conf (dev broker)
deploy/         systemd unit (nxiiot-gateway.service)
```

## Production deployment

Runs natively under systemd (not Docker — needed for `/dev/rtc0` and real `nmcli`). See `deploy/nxiiot-gateway.service` for the reference unit.

`nxiiot-gateway.service` points at `/opt/nxiiot-gateway`, but on a device where `/` is small/shared with other things (e.g. the MaxGate400 — its root partition was already at 77% used from unrelated Docker images, tripping `storage_level: WARNING` even though this install is ~19MB), check for a separate data partition first (`df -h`, `cat /etc/fstab`) and install there instead, with `/opt/nxiiot-gateway` left as a symlink to it — the systemd unit and everything below then need zero changes:

```bash
# only if / is tight and a roomier partition exists (e.g. /userdata) — check first, don't assume
mkdir -p /userdata/nxiiot-gateway
ln -s /userdata/nxiiot-gateway /opt/nxiiot-gateway
```

```bash
# On the target device
git pull   # or otherwise sync this repo
go build -o /tmp/gateway-new ./cmd/gateway   # cross-compiling from another OS isn't used — build natively on the target

# Back up the running binary, then hot-swap it — do NOT `cp` straight onto the target
sudo cp /opt/nxiiot-gateway/gateway /opt/nxiiot-gateway/gateway.bak-$(date +%Y%m%d%H%M%S)
sudo cp /tmp/gateway-new /opt/nxiiot-gateway/gateway.new
sudo mv /opt/nxiiot-gateway/gateway.new /opt/nxiiot-gateway/gateway
sudo systemctl restart nxiiot-gateway

# Verify
sudo systemctl status nxiiot-gateway --no-pager
journalctl -u nxiiot-gateway --no-pager -n 20
```

**Why `mv`, not `cp`, for the hot-swap step**: `cp`ing directly onto `/opt/nxiiot-gateway/gateway` while the service is running it fails with `cannot create regular file: Text file busy` (`ETXTBSY`) — Linux won't let you open-for-write a file that's mapped as a running process's executable text. `mv` (a `rename()`) just repoints the directory entry to a new inode without touching the old one, so the running process is undisturbed until the restart picks up the new binary. See MEMORY.md.

**Any deploy that ships a new migration file must copy it into the deployed `migrations/` directory as an explicit step, separate from the binary swap** — a `git pull` on the target updates the checkout, not necessarily whatever separate directory the running systemd unit points `-migrations` at, if that directory is a deploy-time copy rather than the checkout itself. Verify the new `.sql` file is present there *before* restarting.

## Status

MVP (Phases 0-8 of the original design doc) is complete. Summary:

- **Phase 0-4** (project setup through Store & Forward): done, verified live including crash recovery and zero-duplicate delivery under a real backlog.
- **Phase 5 — MQTT**: `MQTTAdapter` (QoS 1, application-level ack, TLS/auth, auto-reconnect) alongside `HTTPAdapter`, selected via `forwarder.transport`.
- **Phase 6 — Time Service**: hand-rolled SNTP client, Linux hardware RTC (`/dev/rtc0` ioctls), SYNCED/RTC/UNSYNCED/INVALID quality state machine.
- **Phase 8 — Reliability**: chaos-tested — a `SIGKILL` power-failure simulation and a full network partition (which surfaced and fixed a real DNS-classification bug). Cumulative duplicate check across chaos testing on the original deployment: 4747/4747 unique `gateway_id+sequence_id` keys.
- **Post-MVP**: Settings save (Gateway/MQTT/Time, save + auto-restart), host network IP configuration (`internal/netconfig`, Linux/NetworkManager only).
- **Connection/Device split**: physical link (`connection` — protocol/interface/baud/etc.) is separate from `device` (a slave ID + its data points), so several devices can share one physical RTU bus safely (real multi-drop, one goroutine per connection, not per device).
- **Polling model**: continuous round-robin scan across devices on a connection (no per-device/per-datapoint interval gating), each device's datapoints batched into as few Modbus block reads as possible (`internal/acquisition/blockplan.go`).

**Known, honestly-documented gaps** (not silently assumed to work):
- The CRC-error/`DEVICE_OFFLINE` failure paths haven't been triggered from live hardware yet on the original deployment (unplug mid-poll, forced bad response) — the classification logic itself is unit-tested against a real frame decoder, just not exercised from live hardware.
- Host network IP (`internal/netconfig`): `Current()` was verified live against a real device's `nmcli`. `ApplyStatic`/the confirm-or-auto-revert flow was still unverified as of this split — deliberately deferred until physical console access is arranged, since a bad apply could lock out the only remote path to the device.
- RTC: the Linux `/dev/rtc0` ioctl code cross-compiles/unit-tests clean; whether it's been run against a physical RTC chip depends on what's wired to this specific device.
- FC1/FC2 (coil/discrete input) decoding is broken independent of anything recent — `DataType.RegisterCount()`'s byte-width math doesn't match Modbus's bit-packed coil/discrete-input response format. Not currently blocking anything if no coil/discrete-input datapoint is configured, but will bite the first time one is.
- `datapoint.word_order` is a dead field — persisted since the first migration, never read by decode logic. `byte_order` alone (e.g. `"ABCD"`/`"BADC"`) already fully specifies both byte and word order for 32/64-bit types.
- `modbus.ReadWithRetry` is called with `maxRetries=0` hardcoded even though `connection.Connection.Retry` exists, is validated, and is otherwise unused.
