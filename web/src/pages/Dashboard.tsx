import { useEffect, useRef, useState } from 'react'
import { api } from '../api'
import { styles } from '../styles'
import { Icon } from '../icons'
import { fmtBytes, fmtNum } from '../format'
import type { DashboardSummary, StoreForwardStatus, SystemInfo, TimeStatus } from '../types'

function fmtPercent(v?: number) {
  return v === undefined ? '—' : `${v.toFixed(1)}%`
}

// bufferRemainingSeconds estimates how long the queue can keep taking new
// readings before it reaches max_bytes and starts evicting unsent data —
// "if the server went away now, how long until we lose data". Acquisition
// queues every reading whether or not the server is reachable (Rule 1), so
// today's write rate is the rate that would apply during an outage. In V2
// the queue only holds unacknowledged data, so the room left is simply
// max_bytes - queue_bytes, at avg_bytes_per_reading per reading.
//
// Approximate: avg_bytes_per_reading is the compressed chunk size and
// leaves out SQLite page overhead, so the real figure is somewhat lower;
// and the min_free_percent floor can trigger eviction earlier if something
// else fills the disk.
function bufferRemainingSeconds(sf: StoreForwardStatus): number | null {
  if (sf.write_rate_per_sec <= 0 || sf.avg_bytes_per_reading <= 0) return null
  const bytesLeft = sf.max_bytes - sf.queue_bytes
  if (bytesLeft <= 0) return 0
  return bytesLeft / sf.avg_bytes_per_reading / sf.write_rate_per_sec
}

function fmtDuration(seconds: number | null): string {
  if (seconds === null) return '—'
  if (seconds <= 0) return 'now'
  if (seconds < 60) return `${seconds.toFixed(0)}s`
  const minutes = seconds / 60
  if (minutes < 60) return `${minutes.toFixed(0)}m`
  const hours = minutes / 60
  if (hours < 48) return `${hours.toFixed(1)}h`
  return `${(hours / 24).toFixed(1)}d`
}

function timeQualityBadgeStyle(quality?: string) {
  if (quality === 'SYNCED') return styles.badgeGood
  if (quality === 'RTC' || quality === 'UNSYNCED') return styles.badgeNeutral
  return styles.badgeBad
}

export function Dashboard() {
  const [system, setSystem] = useState<SystemInfo | null>(null)
  const [summary, setSummary] = useState<DashboardSummary | null>(null)
  const [storeForward, setStoreForward] = useState<StoreForwardStatus | null>(null)
  const [time, setTime] = useState<TimeStatus | null>(null)
  const [error, setError] = useState<string | null>(null)
  const loading = useRef(false)

  useEffect(() => {
    const load = () => {
      if (loading.current) return
      loading.current = true
      Promise.all([api.getSystem(), api.getDashboardSummary(), api.getStoreForwardStatus(), api.getTime()])
        .then(([sys, sum, sf, t]) => {
          setSystem(sys)
          setSummary(sum)
          setStoreForward(sf)
          setTime(t)
          setError(null)
        })
        .catch((err) => setError(String(err instanceof Error ? err.message : err)))
        .finally(() => {
          loading.current = false
        })
    }

    load()
    const interval = setInterval(load, 5000)
    return () => clearInterval(interval)
  }, [])

  if (error && !system) {
    return (
      <div>
        <h2 style={{ marginTop: 0 }}>Dashboard</h2>
        <div style={styles.errorBox}>Cannot reach gateway API: {error}</div>
      </div>
    )
  }

  if (!system || !summary || !storeForward || !time) {
    return (
      <div>
        <h2 style={{ marginTop: 0 }}>Dashboard</h2>
        <p style={styles.muted}>Loading…</p>
      </div>
    )
  }

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>Dashboard</h2>
      {error && <div style={styles.errorBox}>Last refresh failed: {error}</div>}

      <div style={styles.cardGrid}>
        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="gateway" /></div>
          <div style={styles.cardTitle}>Gateway Status</div>
          <div style={styles.cardValue}>{system.status === 'ok' ? 'Running' : system.status}</div>
          <div style={styles.cardSub}>uptime {(system.uptime_seconds / 60).toFixed(1)} min</div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="cpu" /></div>
          <div style={styles.cardTitle}>CPU</div>
          <div style={styles.cardValue}>{fmtPercent(system.cpu_percent)}</div>
          <div style={styles.progressTrack}>
            <div style={styles.progressFill(system.cpu_percent ?? 0, (system.cpu_percent ?? 0) >= 90)} />
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="ram" /></div>
          <div style={styles.cardTitle}>RAM</div>
          <div style={styles.cardValue}>{fmtPercent(system.mem_used_percent)}</div>
          <div style={styles.progressTrack}>
            <div style={styles.progressFill(system.mem_used_percent ?? 0, (system.mem_used_percent ?? 0) >= 90)} />
          </div>
          <div style={styles.cardSub}>
            {system.mem_used_mb?.toFixed(0)} / {system.mem_total_mb?.toFixed(0)} MB
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="storage" /></div>
          <div style={styles.cardTitle}>Storage</div>
          <div style={styles.cardValue}>{fmtPercent(system.disk_used_percent)}</div>
          <div style={styles.progressTrack}>
            <div style={styles.progressFill(system.disk_used_percent ?? 0, (system.disk_used_percent ?? 0) >= 90)} />
          </div>
          <div style={styles.cardSub}>
            {system.disk_used_gb?.toFixed(1)} / {system.disk_total_gb?.toFixed(1)} GB
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="storage" /></div>
          <div style={styles.cardTitle}>Database Size</div>
          <div style={styles.cardValue}>{fmtBytes(system.database_size_bytes)}</div>
          <div style={styles.cardSub}>gateway.db, incl. WAL</div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="network" /></div>
          <div style={styles.cardTitle}>Network (cumulative)</div>
          <div style={styles.cardValue}>{fmtBytes(system.net_bytes_sent)}</div>
          <div style={styles.cardSub}>sent · {fmtBytes(system.net_bytes_recv)} received</div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="devices" /></div>
          <div style={styles.cardTitle}>Devices</div>
          <div style={styles.cardValue}>
            {summary.enabled_device_count} / {summary.device_count}
          </div>
          <div style={styles.cardSub}>enabled / total</div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="data-points" /></div>
          <div style={styles.cardTitle}>Data Points</div>
          <div style={styles.cardValue}>{fmtNum(summary.data_point_count)}</div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="cloud" /></div>
          <div style={styles.cardTitle}>Server Connection</div>
          <div style={styles.cardValue}>
            <span style={storeForward.server_connected ? styles.badgeGood : styles.badgeBad}>
              <span style={styles.badgeDot} />
              {storeForward.server_connected ? 'Connected' : 'Disconnected'}
            </span>
          </div>
          {storeForward.server_last_error && <div style={styles.cardSub}>{storeForward.server_last_error}</div>}
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="queue" /></div>
          <div style={styles.cardTitle}>Pending Readings</div>
          <div style={styles.cardValue}>{fmtNum(storeForward.pending_readings + storeForward.buffered_readings)}</div>
          <div style={styles.cardSub}>
            not yet acknowledged by the server
            {storeForward.evicted_readings > 0 && ` · ${fmtNum(storeForward.evicted_readings)} evicted when full`}
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="clock" /></div>
          <div style={styles.cardTitle}>Queue Size</div>
          <div style={styles.cardValue}>
            {fmtBytes(storeForward.queue_bytes)} / {fmtBytes(storeForward.max_bytes)}
          </div>
          <div style={styles.cardSub}>
            {storeForward.avg_bytes_per_reading > 0 && `~${storeForward.avg_bytes_per_reading.toFixed(1)} B per reading · `}
            oldest unsent readings are evicted once full
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="activity" /></div>
          <div style={styles.cardTitle}>Queue Write Rate</div>
          <div style={styles.cardValue}>{storeForward.write_rate_per_sec.toFixed(1)} readings/s</div>
          <div style={styles.cardSub}>readings queued per second, averaged over ~30 s</div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="timeout" /></div>
          <div style={styles.cardTitle}>Est. Outage Buffer Remaining</div>
          <div style={styles.cardValue}>{fmtDuration(bufferRemainingSeconds(storeForward))}</div>
          <div style={styles.cardSub}>
            if the server stays unreachable starting now, roughly how long until the queue is full and the oldest
            unsent readings start being evicted
          </div>
        </div>

        <div style={styles.card}>
          <div style={styles.cardIcon}><Icon name="clock" /></div>
          <div style={styles.cardTitle}>Time Synchronization</div>
          <div style={styles.cardValue}>
            <span style={timeQualityBadgeStyle(time.time_quality)}>
              <span style={styles.badgeDot} />
              {time.time_quality}
            </span>
          </div>
          <div style={styles.cardSub}>{time.ntp_server || 'no NTP server configured'}</div>
        </div>
      </div>
    </div>
  )
}
