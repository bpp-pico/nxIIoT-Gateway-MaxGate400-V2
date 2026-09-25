import { useEffect, useRef, useState } from 'react'
import { api } from '../api'
import { styles } from '../styles'
import { Icon } from '../icons'
import type { ConfigImportResult, NetworkStatus, Settings } from '../types'

const MiB = 1024 * 1024

// The gateway only stores/echoes this string (see internal/time/service.go)
// — it does not currently drive any real timezone-aware conversion, the
// gateway operates in UTC internally regardless of this value. A dropdown
// still beats free text: it rules out typos and makes the valid values
// discoverable. Intl.supportedValuesOf gives the full IANA list without
// hand-maintaining one; browsers too old to support it (rare) just fall
// back to a short common list rather than crashing.
function timezoneOptions(current: string): string[] {
  let zones: string[]
  try {
    zones = Intl.supportedValuesOf('timeZone')
  } catch {
    zones = ['UTC', 'Asia/Bangkok', 'Asia/Singapore', 'Asia/Tokyo', 'Europe/London', 'America/New_York', 'America/Los_Angeles']
  }
  if (current && !zones.includes(current)) zones = [current, ...zones]
  return zones
}

export function ConfigPage() {
  return (
    <div>
      <h2>Config</h2>
      <SettingsSection />
      <NetworkSection />
      <BackupRestoreSection />
    </div>
  )
}

// ---------------------------------------------------------------------
// Gateway / MQTT / Time settings — saved into configs/config.yaml, then
// the gateway process restarts to apply them (the "simpler and safer"
// option: no live-reload of the MQTT client or Time Service in place).
// ---------------------------------------------------------------------
// Once loaded, queue is always normalized to a concrete value (see the
// getSettings().then below) even though the wire type leaves it optional
// for backward compatibility with older gateway builds.
type LoadedSettings = Settings & {
  queue: NonNullable<Settings['queue']>
  mqtt: Settings['mqtt'] & { transport: string }
  store_forward: NonNullable<Settings['store_forward']>
}

function SettingsSection() {
  const [settings, setSettings] = useState<LoadedSettings | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [restarting, setRestarting] = useState(false)

  useEffect(() => {
    api
      .getSettings()
      // Default queue.max_bytes/min_free_percent (the gateway's own
      // defaults) when talking to a build that doesn't send them, so the
      // form always has a value to show.
      .then((s) =>
        setSettings({
          ...s,
          queue: { max_bytes: 1024 * MiB, min_free_percent: 10, ...s.queue },
          mqtt: { ...s.mqtt, transport: s.mqtt.transport ?? 'mqtt' },
          store_forward: s.store_forward ?? { enabled: true },
        }),
      )
      .catch((err) => setError(String(err instanceof Error ? err.message : err)))
  }, [])

  // Once the save triggers a restart, poll /api/system until it answers
  // again (a fresh process, higher-or-reset uptime) so the page doesn't
  // just say "restarting" forever.
  useEffect(() => {
    if (!restarting) return
    const interval = setInterval(() => {
      api
        .getSystem()
        .then(() => {
          setRestarting(false)
          setSaving(false)
        })
        .catch(() => {
          // still restarting — expected, keep polling
        })
    }, 1000)
    return () => clearInterval(interval)
  }, [restarting])

  const handleSave = async () => {
    if (!settings) return
    if (!confirm('Save settings and restart the gateway now? The gateway will be briefly unreachable.')) return

    setSaving(true)
    setError(null)
    try {
      await api.saveSettings(settings)
      setRestarting(true)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
      setSaving(false)
    }
  }

  if (error && !settings) {
    return (
      <>
        <div style={styles.sectionTitle}>Gateway / MQTT / Time Settings</div>
        <div style={styles.errorBox}>{error}</div>
      </>
    )
  }
  if (!settings) {
    return (
      <>
        <div style={styles.sectionTitle}>Gateway / MQTT / Time Settings</div>
        <p style={styles.muted}>Loading…</p>
      </>
    )
  }

  return (
    <>
      <div style={styles.sectionTitle}>Gateway / MQTT / Time Settings</div>
      <p style={styles.muted}>
        Saving restarts the gateway process to apply changes (a few seconds of downtime). This rewrites
        configs/config.yaml — any hand-added comments in that file are lost once saved from here.
      </p>
      {error && <div style={styles.errorBox}>{error}</div>}
      {restarting && <div style={{ ...styles.errorBox, background: '#FFF7E6', color: '#B45309' }}>Restarting gateway…</div>}

      <div style={styles.cardGrid}>
        <div style={styles.card}>
          <div style={styles.cardTitle}>Gateway</div>
          <div style={styles.formRow}>
            <label style={styles.label}>Gateway ID</label>
            <input
              style={styles.input}
              value={settings.gateway.id}
              onChange={(e) => setSettings({ ...settings, gateway: { ...settings.gateway, id: e.target.value } })}
            />
          </div>
          <div style={styles.formRow}>
            <label style={styles.label}>Name</label>
            <input
              style={styles.input}
              value={settings.gateway.name}
              onChange={(e) => setSettings({ ...settings, gateway: { ...settings.gateway, name: e.target.value } })}
            />
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardTitle}>Store &amp; Forward</div>
          <div style={{ ...styles.formRow, flexDirection: 'row', alignItems: 'center', gap: '0.5rem' }}>
            <input
              type="checkbox"
              id="store-forward-enabled"
              checked={settings.store_forward.enabled}
              onChange={(e) => setSettings({ ...settings, store_forward: { enabled: e.target.checked } })}
            />
            <label htmlFor="store-forward-enabled" style={styles.label}>
              Enabled
            </label>
          </div>
          <p style={{ ...styles.muted, marginTop: -8 }}>
            {settings.store_forward.enabled
              ? 'Readings are saved locally and sent to the MQTT/HTTP server below (default behavior).'
              : 'Modbus read-only mode: readings are polled and shown live (Dashboard, Logs) but never saved locally or sent anywhere — no MQTT/HTTP connection is made at all. The fields below are ignored while this is off.'}
          </p>
        </div>

        <div style={styles.card}>
          <div style={styles.cardTitle}>MQTT Server</div>
          <div style={styles.formRow}>
            <label style={styles.label}>Transport</label>
            <select
              style={styles.select}
              value={settings.mqtt.transport}
              onChange={(e) => setSettings({ ...settings, mqtt: { ...settings.mqtt, transport: e.target.value } })}
            >
              <option value="mqtt">MQTT (production)</option>
              <option value="http">HTTP (dev/test only)</option>
            </select>
          </div>
          {settings.mqtt.transport === 'mqtt' && (
            <p style={{ ...styles.muted, marginTop: -8 }}>
              Saving with MQTT selected briefly connects to Broker URL below to confirm it's reachable before writing
              anything — if it can't connect, the save is rejected with an error and nothing changes. (A broker that
              goes down later can still cause a restart failure — this only checks reachability at save time.)
            </p>
          )}
          <div style={styles.formRow}>
            <label style={styles.label}>Broker URL</label>
            <input
              style={styles.input}
              placeholder="tcp://broker.internal:1883"
              value={settings.mqtt.broker_url}
              onChange={(e) => setSettings({ ...settings, mqtt: { ...settings.mqtt, broker_url: e.target.value } })}
            />
          </div>
          <div style={styles.formRow}>
            <label style={styles.label}>Client ID</label>
            <input
              style={styles.input}
              value={settings.mqtt.client_id}
              onChange={(e) => setSettings({ ...settings, mqtt: { ...settings.mqtt, client_id: e.target.value } })}
            />
          </div>
          <div style={styles.formRow}>
            <label style={styles.label}>Username</label>
            <input
              style={styles.input}
              value={settings.mqtt.username ?? ''}
              onChange={(e) => setSettings({ ...settings, mqtt: { ...settings.mqtt, username: e.target.value } })}
            />
          </div>
          <div style={styles.formRow}>
            <label style={styles.label}>Password (leave blank to keep unchanged)</label>
            <input
              type="password"
              style={styles.input}
              value={settings.mqtt.password ?? ''}
              onChange={(e) => setSettings({ ...settings, mqtt: { ...settings.mqtt, password: e.target.value } })}
            />
          </div>
          <div style={styles.formRow}>
            <label style={styles.label}>QoS</label>
            <input
              type="number"
              min={0}
              max={2}
              style={styles.input}
              value={settings.mqtt.qos}
              onChange={(e) => setSettings({ ...settings, mqtt: { ...settings.mqtt, qos: Number(e.target.value) } })}
            />
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardTitle}>Time / NTP</div>
          <div style={styles.formRow}>
            <label style={styles.label}>NTP Server</label>
            <input
              style={styles.input}
              placeholder="ntp.internal (empty disables NTP sync)"
              value={settings.time.ntp_server}
              onChange={(e) => setSettings({ ...settings, time: { ...settings.time, ntp_server: e.target.value } })}
            />
          </div>
          <div style={styles.formRow}>
            <label style={styles.label}>Timezone</label>
            <select
              style={styles.select}
              value={settings.time.timezone}
              onChange={(e) => setSettings({ ...settings, time: { ...settings.time, timezone: e.target.value } })}
            >
              {timezoneOptions(settings.time.timezone).map((tz) => (
                <option key={tz} value={tz}>
                  {tz}
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formRow}>
            <label style={styles.label}>Sync Interval (seconds)</label>
            <input
              type="number"
              min={1}
              style={styles.input}
              value={settings.time.sync_interval_seconds}
              onChange={(e) =>
                setSettings({ ...settings, time: { ...settings.time, sync_interval_seconds: Number(e.target.value) } })
              }
            />
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardTitle}>Store &amp; Forward</div>
          <div style={styles.formRow}>
            <label style={styles.label}>Max Queue Size (MiB)</label>
            <input
              type="number"
              min={1}
              style={styles.input}
              value={Math.round(settings.queue.max_bytes / MiB)}
              onChange={(e) =>
                setSettings({ ...settings, queue: { ...settings.queue, max_bytes: Number(e.target.value) * MiB } })
              }
            />
          </div>
          <p style={{ ...styles.muted, marginTop: '0.35rem', marginBottom: 0 }}>
            Size cap for the local queue file. Readings wait there until the server acknowledges them; once the
            cap is reached the oldest unsent readings are deleted to make room.
          </p>
          <div style={{ ...styles.formRow, marginTop: '0.75rem' }}>
            <label style={styles.label}>Minimum Free Disk Space (%)</label>
            <input
              type="number"
              min={1}
              max={50}
              style={styles.input}
              value={settings.queue.min_free_percent}
              onChange={(e) =>
                setSettings({ ...settings, queue: { ...settings.queue, min_free_percent: Number(e.target.value) } })
              }
            />
          </div>
          <p style={{ ...styles.muted, marginTop: '0.35rem', marginBottom: 0 }}>
            Space always kept free on the queue's disk, whatever else is using it. Below this the oldest queued
            readings are deleted too — a completely full disk would stop the gateway from storing anything.
          </p>
        </div>
      </div>

      <button style={{ ...styles.primaryButton, marginTop: '1rem' }} disabled={saving} onClick={handleSave}>
        {saving ? 'Saving…' : 'Save & Restart Gateway'}
      </button>
    </>
  )
}

// ---------------------------------------------------------------------
// Host network IP — only meaningful when the gateway binary runs
// directly on a Linux host with NetworkManager (the systemd deployment
// path), never inside this project's own Docker Compose dev containers.
// A confirm-or-auto-revert safety net (backend: internal/netconfig)
// protects against a typo'd IP/gateway locking the device out.
// ---------------------------------------------------------------------
function NetworkSection() {
  const [status, setStatus] = useState<NetworkStatus | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [applying, setApplying] = useState(false)
  const [confirmDeadline, setConfirmDeadline] = useState<number | null>(null)
  const [now, setNow] = useState(Date.now())
  const [form, setForm] = useState({ interface: '', address: '', prefix: 24, gateway: '', dns: '' })
  const [mode, setMode] = useState<'static' | 'dhcp'>('static')

  const load = () => {
    api
      .getNetworkStatus()
      .then((s) => {
        setStatus(s)
        setError(null)
        if (s.pending_confirmation === false) setConfirmDeadline(null)
        if (!form.interface && s.interface) {
          setForm({
            interface: s.interface,
            address: s.address ?? '',
            prefix: s.prefix ?? 24,
            gateway: s.gateway ?? '',
            dns: (s.dns ?? []).join(', '),
          })
          // Default the mode tab to whatever the host is actually running,
          // so switching to it doesn't look like a change you have to Apply.
          setMode(s.method === 'auto' ? 'dhcp' : 'static')
        }
      })
      .catch((err) => setError(String(err instanceof Error ? err.message : err)))
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (confirmDeadline === null) return
    const interval = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(interval)
  }, [confirmDeadline])

  const secondsLeft = confirmDeadline !== null ? Math.max(0, Math.round((confirmDeadline - now) / 1000)) : 0

  useEffect(() => {
    if (confirmDeadline !== null && secondsLeft === 0) {
      // The revert window has elapsed server-side; refresh to show the
      // (now reverted) actual state.
      setConfirmDeadline(null)
      load()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [secondsLeft])

  const handleApply = async () => {
    if (!confirm(`Apply static IP ${form.address}/${form.prefix}? If this address becomes unreachable it will auto-revert.`))
      return
    setApplying(true)
    setError(null)
    try {
      const dns = form.dns
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
      const result = await api.applyNetwork({
        interface: form.interface,
        address: form.address,
        prefix: form.prefix,
        gateway: form.gateway,
        dns,
      })
      setConfirmDeadline(Date.now() + result.confirm_within_seconds * 1000)
      setNow(Date.now())
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    } finally {
      setApplying(false)
    }
  }

  const handleApplyDHCP = async () => {
    if (!confirm(`Switch ${form.interface} to DHCP (automatic)? If this address becomes unreachable it will auto-revert.`))
      return
    setApplying(true)
    setError(null)
    try {
      const result = await api.applyNetworkDHCP({ interface: form.interface })
      setConfirmDeadline(Date.now() + result.confirm_within_seconds * 1000)
      setNow(Date.now())
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    } finally {
      setApplying(false)
    }
  }

  const handleConfirm = async () => {
    try {
      await api.confirmNetwork()
      setConfirmDeadline(null)
      load()
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
  }

  return (
    <>
      <div style={styles.sectionTitle}>Network (Host IP)</div>

      {!status ? (
        !error && <p style={styles.muted}>Loading…</p>
      ) : !status.supported ? (
        <p style={styles.muted}>
          Not available on this host — setting the host's real network IP requires Linux with NetworkManager
          (<code>nmcli</code>) and only makes sense when the gateway runs directly on the host, not inside this
          project's Docker Compose dev containers (a container's network is not the host's real network adapter).
        </p>
      ) : (
        <>
          <p style={styles.muted}>
            Applying takes effect immediately. If not confirmed within the countdown, it automatically reverts to
            the previous configuration — reconnect at the new address and confirm from there before it expires.
          </p>
          {error && <div style={styles.errorBox}>{error}</div>}

          {confirmDeadline !== null && (
            <div style={{ ...styles.errorBox, background: '#FFF7E6', color: '#B45309', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span>New IP applied — confirm within {secondsLeft}s or it will revert automatically.</span>
              <button style={styles.primaryButton} onClick={handleConfirm}>
                Confirm New IP
              </button>
            </div>
          )}

          <div style={styles.cardGrid}>
            <div style={styles.card}>
              <div style={styles.cardTitle}>Current</div>
              <table style={styles.table}>
                <tbody>
                  <tr>
                    <td style={styles.td}>Interface</td>
                    <td style={styles.td}>{status.interface}</td>
                  </tr>
                  <tr>
                    <td style={styles.td}>Method</td>
                    <td style={styles.td}>{status.method}</td>
                  </tr>
                  <tr>
                    <td style={styles.td}>Address</td>
                    <td style={styles.td}>
                      {status.address}
                      {status.prefix ? `/${status.prefix}` : ''}
                    </td>
                  </tr>
                  <tr>
                    <td style={styles.td}>Gateway</td>
                    <td style={styles.td}>{status.gateway}</td>
                  </tr>
                  <tr>
                    <td style={styles.td}>DNS</td>
                    <td style={styles.td}>{(status.dns ?? []).join(', ')}</td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div style={styles.card}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                <div style={{ ...styles.cardTitle, marginBottom: 0 }}>Set Address</div>
                <div style={{ display: 'flex', gap: 2, background: '#FAFAFF', padding: 3, borderRadius: 10, border: '1px solid #ECE9F7' }}>
                  <button
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      fontFamily: 'inherit',
                      fontSize: '0.75rem',
                      fontWeight: mode === 'static' ? 700 : 500,
                      padding: '0.3rem 0.7rem',
                      borderRadius: 7,
                      background: mode === 'static' ? '#F3EFFE' : 'transparent',
                      color: mode === 'static' ? '#8B5CF6' : '#6B6580',
                    }}
                    onClick={() => setMode('static')}
                  >
                    Static
                  </button>
                  <button
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      fontFamily: 'inherit',
                      fontSize: '0.75rem',
                      fontWeight: mode === 'dhcp' ? 700 : 500,
                      padding: '0.3rem 0.7rem',
                      borderRadius: 7,
                      background: mode === 'dhcp' ? '#F3EFFE' : 'transparent',
                      color: mode === 'dhcp' ? '#8B5CF6' : '#6B6580',
                    }}
                    onClick={() => setMode('dhcp')}
                  >
                    DHCP
                  </button>
                </div>
              </div>

              <div style={styles.formRow}>
                <label style={styles.label}>Interface</label>
                <input style={styles.input} value={form.interface} onChange={(e) => setForm({ ...form, interface: e.target.value })} />
              </div>

              {mode === 'static' ? (
                <>
                  <div style={styles.formRow}>
                    <label style={styles.label}>IP Address</label>
                    <input style={styles.input} value={form.address} onChange={(e) => setForm({ ...form, address: e.target.value })} />
                  </div>
                  <div style={styles.formRow}>
                    <label style={styles.label}>Prefix (CIDR)</label>
                    <input
                      type="number"
                      min={1}
                      max={32}
                      style={styles.input}
                      value={form.prefix}
                      onChange={(e) => setForm({ ...form, prefix: Number(e.target.value) })}
                    />
                  </div>
                  <div style={styles.formRow}>
                    <label style={styles.label}>Gateway</label>
                    <input style={styles.input} value={form.gateway} onChange={(e) => setForm({ ...form, gateway: e.target.value })} />
                  </div>
                  <div style={styles.formRow}>
                    <label style={styles.label}>DNS (comma-separated)</label>
                    <input style={styles.input} value={form.dns} onChange={(e) => setForm({ ...form, dns: e.target.value })} />
                  </div>
                  <button style={styles.primaryButton} disabled={applying || confirmDeadline !== null} onClick={handleApply}>
                    {applying ? 'Applying…' : 'Apply Static IP'}
                  </button>
                </>
              ) : (
                <>
                  <p style={styles.muted}>
                    The interface will request an address automatically from DHCP. Protected by the same
                    confirm-or-revert countdown as a static apply.
                  </p>
                  <button style={styles.primaryButton} disabled={applying || confirmDeadline !== null} onClick={handleApplyDHCP}>
                    {applying ? 'Switching…' : 'Switch to DHCP'}
                  </button>
                </>
              )}
            </div>
          </div>
        </>
      )}
    </>
  )
}

// ---------------------------------------------------------------------
// Config backup/restore — unchanged from Phase 7.
// ---------------------------------------------------------------------
function BackupRestoreSection() {
  const [error, setError] = useState<string | null>(null)
  const [importing, setImporting] = useState(false)
  const [importResult, setImportResult] = useState<ConfigImportResult | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const handleExport = async () => {
    setError(null)
    try {
      const data = await api.exportConfig()
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `gateway-config-${data.gateway.id || 'export'}.json`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
  }

  const handleImportFile = async (file: File) => {
    setImporting(true)
    setImportResult(null)
    setError(null)
    try {
      const text = await file.text()
      const payload = JSON.parse(text)
      const result = await api.importConfig(payload)
      setImportResult(result)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    } finally {
      setImporting(false)
    }
  }

  return (
    <>
      <div style={styles.sectionTitle}>Backup</div>
      {error && <div style={styles.errorBox}>{error}</div>}
      <p style={styles.muted}>
        Downloads devices, data points, and non-secret gateway settings as JSON. Passwords are never included.
      </p>
      <button style={{ ...styles.button, display: 'inline-flex', alignItems: 'center', gap: 8 }} onClick={handleExport}>
        <Icon name="download" size={15} color="#8B5CF6" />
        Export Configuration
      </button>

      <div style={styles.sectionTitle}>Restore</div>
      <p style={styles.muted}>
        Restores devices and data points from a previously exported file (matched by id — an id that doesn't exist on
        this gateway is created fresh). Gateway/MQTT/Time settings are not part of this file — use the Settings
        section above for those.
      </p>
      <input
        ref={fileInputRef}
        type="file"
        accept="application/json"
        style={{ display: 'none' }}
        onChange={(e) => {
          const file = e.target.files?.[0]
          if (file) handleImportFile(file)
          e.target.value = ''
        }}
      />
      <button style={{ ...styles.button, display: 'inline-flex', alignItems: 'center', gap: 8 }} disabled={importing} onClick={() => fileInputRef.current?.click()}>
        <Icon name="upload" size={15} color="#8B5CF6" />
        {importing ? 'Importing…' : 'Import Configuration'}
      </button>

      {importResult && (
        <table style={{ ...styles.table, marginTop: '1rem' }}>
          <tbody>
            <tr>
              <td style={styles.td}>Connections created</td>
              <td style={styles.td}>{importResult.connections_created}</td>
            </tr>
            <tr>
              <td style={styles.td}>Connections updated</td>
              <td style={styles.td}>{importResult.connections_updated}</td>
            </tr>
            <tr>
              <td style={styles.td}>Devices created</td>
              <td style={styles.td}>{importResult.devices_created}</td>
            </tr>
            <tr>
              <td style={styles.td}>Devices updated</td>
              <td style={styles.td}>{importResult.devices_updated}</td>
            </tr>
            <tr>
              <td style={styles.td}>Data points created</td>
              <td style={styles.td}>{importResult.data_points_created}</td>
            </tr>
            <tr>
              <td style={styles.td}>Data points updated</td>
              <td style={styles.td}>{importResult.data_points_updated}</td>
            </tr>
            {importResult.errors && importResult.errors.length > 0 && (
              <tr>
                <td style={styles.td}>Errors</td>
                <td style={styles.td}>
                  <ul style={{ margin: 0, paddingLeft: '1.2rem' }}>
                    {importResult.errors.map((e, i) => (
                      <li key={i}>{e}</li>
                    ))}
                  </ul>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </>
  )
}
