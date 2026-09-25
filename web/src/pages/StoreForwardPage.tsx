import { useEffect, useRef, useState } from 'react'
import { api } from '../api'
import { styles } from '../styles'
import { Icon } from '../icons'
import { fmtBytes, fmtNum } from '../format'
import type { StoreForwardStatus } from '../types'

function fmtTime(v?: string) {
  return v ? new Date(v).toLocaleString() : '—'
}

function storageLevelBadgeStyle(level?: string) {
  if (level === 'FULL' || level === 'CRITICAL') return styles.badgeBad
  if (level === 'WARNING') return styles.badgeNeutral
  return styles.badgeGood
}

export function StoreForwardPage() {
  const [status, setStatus] = useState<StoreForwardStatus | null>(null)
  const [error, setError] = useState<string | null>(null)
  const loading = useRef(false)

  useEffect(() => {
    const load = () => {
      if (loading.current) return
      loading.current = true
      api
        .getStoreForwardStatus()
        .then((s) => {
          setStatus(s)
          setError(null)
        })
        .catch((err) => setError(String(err instanceof Error ? err.message : err)))
        .finally(() => {
          loading.current = false
        })
    }
    load()
    const interval = setInterval(load, 3000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>Store &amp; Forward</h2>
      {error && <div style={styles.errorBox}>{error}</div>}

      {!status ? (
        !error && <p style={styles.muted}>Loading…</p>
      ) : (
        <>
          <div style={styles.cardGrid}>
            <div style={styles.card}>
              <div style={styles.cardIcon}><Icon name="cloud" /></div>
              <div style={styles.cardTitle}>Server Status</div>
              <div style={styles.cardValue}>
                <span style={status.server_connected ? styles.badgeGood : styles.badgeBad}>
                  <span style={styles.badgeDot} />
                  {status.server_connected ? 'Connected' : 'Disconnected'}
                </span>
              </div>
              {status.server_last_error && <div style={styles.cardSub}>{status.server_last_error}</div>}
            </div>

            <div style={styles.card}>
              <div style={styles.cardIcon}><Icon name="queue" /></div>
              <div style={styles.cardTitle}>Pending Readings</div>
              <div style={styles.cardValue}>{fmtNum(status.pending_readings)}</div>
              <div style={styles.cardSub}>stored, waiting for the server's ack · {fmtNum(status.pending_chunks)} chunks</div>
            </div>

            <div style={styles.card}>
              <div style={styles.cardIcon}><Icon name="sending" /></div>
              <div style={styles.cardTitle}>In Memory</div>
              <div style={styles.cardValue}>{fmtNum(status.buffered_readings)}</div>
              <div style={styles.cardSub}>written to disk at the next flush (every ~2 s)</div>
            </div>

            <div style={styles.card}>
              <div style={styles.cardIcon}><Icon name="retry" /></div>
              <div style={styles.cardTitle}>Readings Lost</div>
              <div style={styles.cardValue}>{fmtNum(status.evicted_readings + status.dropped_readings)}</div>
              <div style={styles.cardSub}>
                {fmtNum(status.evicted_readings)} evicted when the queue was full · {fmtNum(status.dropped_readings)}{' '}
                not written to disk since start
              </div>
            </div>

            <div style={styles.card}>
              <div style={styles.cardIcon}><Icon name="clock" /></div>
              <div style={styles.cardTitle}>Queue Size</div>
              <div style={styles.cardValue}>
                {fmtBytes(status.queue_bytes)} / {fmtBytes(status.max_bytes)}
              </div>
              <div style={styles.cardSub}>oldest unsent readings are evicted once full</div>
            </div>

            <div style={styles.card}>
              <div style={styles.cardIcon}><Icon name="storage" /></div>
              <div style={styles.cardTitle}>Storage Usage</div>
              <div style={styles.cardValue}>
                {status.storage_used_percent !== undefined ? `${status.storage_used_percent.toFixed(1)}%` : '—'}
                {status.storage_level && (
                  <span style={{ ...storageLevelBadgeStyle(status.storage_level), marginLeft: '0.5rem', verticalAlign: 'middle' }}>
                    <span style={styles.badgeDot} />
                    {status.storage_level}
                  </span>
                )}
              </div>
              {status.storage_used_percent !== undefined && (
                <div style={styles.progressTrack}>
                  <div style={styles.progressFill(status.storage_used_percent, status.storage_used_percent >= 90)} />
                </div>
              )}
            </div>
          </div>

          <div style={{ marginTop: '1.5rem', background: '#fff', border: '1px solid #ECE9F7', borderRadius: 20, padding: '0.25rem 1.25rem' }}>
            <table style={{ ...styles.table, tableLayout: 'fixed' }}>
              <tbody>
                <tr>
                  <td style={{ ...styles.td, color: '#6B6580', width: 260 }}>Oldest Pending Reading</td>
                  <td style={styles.td}>{fmtTime(status.oldest_pending)}</td>
                </tr>
                <tr>
                  <td style={{ ...styles.td, color: '#6B6580', width: 260 }}>Newest Pending Reading</td>
                  <td style={styles.td}>{fmtTime(status.newest_pending)}</td>
                </tr>
                <tr>
                  <td style={{ ...styles.td, color: '#6B6580', width: 260 }}>Last Sent to Server</td>
                  <td style={styles.td}>{fmtTime(status.server_last_sent_at)}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
