// Package acquisition schedules polling of configured Modbus devices and
// emits Readings. It must never depend on Server/queue availability (Rule 1)
// — it only produces Readings via a callback; what happens to them
// downstream (Phase 3/4) is not this package's concern.
package acquisition

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"nxiiot-gateway/internal/connection"
	"nxiiot-gateway/internal/datapoint"
	"nxiiot-gateway/internal/device"
	"nxiiot-gateway/internal/diagnostics"
	"nxiiot-gateway/internal/modbus"
)

// OnReading is called for every acquired (or failed) Data Point read. It
// must return quickly; slow consumers should buffer internally.
type OnReading func(Reading)

// OnPollCycle is called once per device after its data points have been
// polled for the current round-robin pass, reporting how long that took —
// for the Web UI's per-device polling-timing display, so a connection's
// next_device_delay_ms can be tuned against real hardware response times
// instead of guessed. blockReads is how many physical Modbus requests
// (planBlockReads' block plan, one ReadWithRetry call each — success or
// failure both count, each is one real read) it took to cover
// datapointsRead data points this pass.
type OnPollCycle func(deviceID int64, durationMs int64, datapointsRead int, blockReads int, at time.Time)

// reconnectFloorDelay is the minimum wait between Connect() retries after a
// connection failure, used when conn.NextDeviceDelayMs is very small — the
// continuous scan loop has no ticker to naturally pace reconnect attempts
// anymore, so this stops a down broker/bus from being hammered in a tight
// spin loop. Not a const so tests can shrink it instead of running real-time
// for a full second.
var reconnectFloorDelay = time.Second

// cycleYieldDelay is waited once after every full round-robin pass over all
// of a connection's devices (in addition to conn.NextDeviceDelayMs between
// each device) — a small, deliberate breathing gap so the acquisition
// goroutine doesn't compete as continuously for CPU/DB-connection
// scheduling against the forwarder, retention sweep, and Web UI's own DB
// reads, all of which share the gateway's single SQLite database (see
// MEMORY.md's 2026-09-02 SD-card I/O contention entry). Not a const so
// tests can shrink it instead of paying it on every simulated pass.
var cycleYieldDelay = 20 * time.Millisecond

// deviceWithPoints pairs a device with its enabled data points, for polling
// within one connection's shared goroutine.
type deviceWithPoints struct {
	device device.Device
	dps    []datapoint.DataPoint
}

type Poller struct {
	log         *slog.Logger
	onReading   OnReading
	onPollCycle OnPollCycle
	diag        *diagnostics.Store
}

// diag may be nil (e.g. in tests that don't care about diagnostics
// counters) — every diag.RecordResult call below is guarded accordingly.
// onPollCycle may also be nil (e.g. in tests) — every call below is guarded.
func NewPoller(log *slog.Logger, onReading OnReading, onPollCycle OnPollCycle, diag *diagnostics.Store) *Poller {
	return &Poller{log: log, onReading: onReading, onPollCycle: onPollCycle, diag: diag}
}

// runConnection owns exactly one Client for the lifetime of conn's polling
// — one goroutine per physical connection (not per device) is what makes
// sharing that Client across several devices at different slave IDs safe:
// nothing else ever touches it concurrently. This is the fix for a real
// live incident (see MEMORY.md, gateway/migrations/0004_connection_split.sql):
// before the connection/device split, two devices sharing one RTU interface
// each opened their own independent connection, and their Modbus frames
// corrupted each other on the wire.
func (p *Poller) runConnection(ctx context.Context, conn connection.Connection, devices []deviceWithPoints) {
	client, err := BuildClient(conn)
	if err != nil {
		p.log.Error("invalid connection configuration", "connection", conn.Name, "error", err)
		return
	}
	p.runConnectionWithClient(ctx, conn, devices, client)
}

// runConnectionWithClient is runConnection's body, taking an already-built
// Client so tests can substitute a fake instead of a real serial/TCP handle.
//
// Continuous round-robin scan: devices are polled back-to-back, in order,
// wrapping to the first device after the last, with no ticker and no
// per-device/per-datapoint interval gating (see gateway/migrations/
// 0005_scan_polling.sql — replaces the old two-level polling_interval_ms
// scheme). The only pacing is conn.NextDeviceDelayMs, waited between
// devices to give the bus time to settle (RS-485 turnaround, primarily).
func (p *Poller) runConnectionWithClient(ctx context.Context, conn connection.Connection, devices []deviceWithPoints, client modbus.Client) {
	connected := false
	readCtxTimeout := time.Duration(conn.TimeoutMs) * time.Millisecond
	delay := time.Duration(conn.NextDeviceDelayMs) * time.Millisecond

	// wait blocks for d, but returns early (false) if ctx is cancelled —
	// used both for the inter-device delay and the reconnect floor delay,
	// deliberately never a blind time.Sleep, so shutdown/Reload stays
	// responsive even while "waiting."
	wait := func(d time.Duration) bool {
		if d <= 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}

	for {
		if ctx.Err() != nil {
			_ = client.Close()
			return
		}

		if !connected {
			if err := client.Connect(); err != nil {
				now := time.Now().UTC()
				for _, dg := range devices {
					p.emitDeviceOffline(dg.device, dg.dps, now, err)
				}
				if !wait(reconnectFloorDelay) {
					_ = client.Close()
					return
				}
				continue
			}
			connected = true
		}

		for _, dg := range devices {
			if ctx.Err() != nil {
				_ = client.Close()
				return
			}

			client.SetUnitID(byte(dg.device.SlaveID))
			now := time.Now().UTC()
			deviceStart := time.Now()
			readCount, blockReads := p.readDevice(ctx, client, dg.device, dg.dps, readCtxTimeout, now)

			if readCount > 0 {
				if p.onPollCycle != nil {
					p.onPollCycle(dg.device.ID, time.Since(deviceStart).Milliseconds(), readCount, blockReads, now)
				}
				if !wait(delay) {
					_ = client.Close()
					return
				}
			}
		}

		if !wait(cycleYieldDelay) {
			_ = client.Close()
			return
		}
	}
}

// readDevice performs all of one device's planned block reads for a single
// pass and returns how many datapoints were actually read (0 on a
// connection-level failure — the caller treats that as "no bus traffic
// occurred," skipping the inter-device delay) and how many physical Modbus
// requests (one ReadWithRetry call per planBlockReads block, success or
// failure both count as one real read) that took.
func (p *Poller) readDevice(ctx context.Context, client modbus.Client, d device.Device, dps []datapoint.DataPoint, readCtxTimeout time.Duration, eventTime time.Time) (readCount int, blockReads int) {
	for _, block := range planBlockReads(dps) {
		blockReads++
		readCtx, cancel := context.WithTimeout(ctx, readCtxTimeout)
		start := time.Now()
		raw, attempts, err := modbus.ReadWithRetry(readCtx, client, block.functionCode, block.startAddress, block.quantity, 0)
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			q := modbus.QualityFromError(err)
			p.log.Warn("modbus read failed", "device", d.Name, "address", block.startAddress, "quantity", block.quantity, "quality", q, "error", err)
			if p.diag != nil {
				p.diag.RecordResult(q, elapsed, attempts)
			}
			for _, dp := range block.points {
				p.onReading(p.badReading(d, dp, eventTime, q))
			}
			readCount += len(block.points)
			continue
		}
		if p.diag != nil {
			p.diag.RecordResult(modbus.Good, elapsed, attempts)
		}

		for _, dp := range block.points {
			p.decodeAndEmit(d, dp, raw, block.startAddress, eventTime)
			readCount++
		}
	}
	return readCount, blockReads
}

// decodeAndEmit slices dp's own bytes out of a block read's shared raw
// buffer (offset by how far dp's register address sits from the block's
// start) and decodes/scales/emits it exactly as a dedicated single-datapoint
// read would have.
func (p *Poller) decodeAndEmit(d device.Device, dp datapoint.DataPoint, raw []byte, blockStart uint16, eventTime time.Time) {
	dt := modbus.DataType(dp.DataType)
	width, err := dt.ByteWidth()
	if err != nil {
		p.log.Error("invalid data point configuration", "device", d.Name, "tag", dp.TagName, "error", err)
		p.onReading(p.badReading(d, dp, eventTime, modbus.Invalid))
		return
	}

	offset := int(dp.RegisterAddress-blockStart) * 2
	if offset < 0 || offset+width > len(raw) {
		p.log.Error("data point falls outside its planned block read", "device", d.Name, "tag", dp.TagName)
		p.onReading(p.badReading(d, dp, eventTime, modbus.Invalid))
		return
	}

	decoded, err := modbus.Decode(raw[offset:offset+width], dt, dp.ByteOrder)
	if err != nil {
		p.log.Error("decode failed", "device", d.Name, "tag", dp.TagName, "error", err)
		p.onReading(p.badReading(d, dp, eventTime, modbus.Invalid))
		return
	}

	value := modbus.ApplyScale(decoded, dp.Scale, dp.Offset)
	p.onReading(Reading{
		DeviceID:       d.ID,
		DeviceName:     d.Name,
		DatapointID:    dp.ID,
		Tag:            dp.TagName,
		Value:          &value,
		Quality:        modbus.Good,
		Unit:           dp.Unit,
		EventTimestamp: eventTime,
	})
}

func (p *Poller) emitDeviceOffline(d device.Device, dps []datapoint.DataPoint, now time.Time, err error) {
	q := modbus.QualityFromError(err)
	p.log.Warn("device connection failed", "device", d.Name, "quality", q, "error", err)
	for _, dp := range dps {
		p.onReading(p.badReading(d, dp, now, q))
	}
}

func (p *Poller) badReading(d device.Device, dp datapoint.DataPoint, eventTime time.Time, q modbus.Quality) Reading {
	return Reading{
		DeviceID:       d.ID,
		DeviceName:     d.Name,
		DatapointID:    dp.ID,
		Tag:            dp.TagName,
		Value:          nil,
		Quality:        q,
		Unit:           dp.Unit,
		EventTimestamp: eventTime,
	}
}

// BuildClient constructs a Modbus client for a connection's configured
// protocol. Exported so the API layer can build short-lived clients for
// "Test Connection" / "Test Read" without touching the Manager's
// long-lived polling connections. It does not set a slave/unit ID —
// callers call Client.SetUnitID for whichever device they're about to
// read, since one Connection can serve several devices at different IDs.
func BuildClient(c connection.Connection) (modbus.Client, error) {
	timeout := time.Duration(c.TimeoutMs) * time.Millisecond

	switch c.Protocol {
	case connection.TCP:
		if c.IPAddress == "" {
			return nil, fmt.Errorf("TCP connection %q has no ip_address configured", c.Name)
		}
		addr := fmt.Sprintf("%s:%d", c.IPAddress, c.Port)
		return modbus.NewTCPClient(modbus.TCPConfig{
			Address: addr,
			Timeout: timeout,
		}), nil

	case connection.RTU:
		if c.Interface == "" {
			return nil, fmt.Errorf("RTU connection %q has no interface configured", c.Name)
		}
		return modbus.NewRTUClient(modbus.RTUConfig{
			Interface: c.Interface,
			BaudRate:  c.BaudRate,
			DataBits:  c.DataBits,
			Parity:    c.Parity,
			StopBits:  c.StopBits,
			Timeout:   timeout,
		}), nil

	default:
		return nil, fmt.Errorf("unknown protocol %q", c.Protocol)
	}
}
