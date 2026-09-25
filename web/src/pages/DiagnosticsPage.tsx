import { useEffect, useState } from 'react'
import { api } from '../api'
import { styles } from '../styles'
import { Icon } from '../icons'
import { fmtNum } from '../format'
import type { Diagnostics } from '../types'

export function DiagnosticsPage() {
  const [diag, setDiag] = useState<Diagnostics | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const load = () => {
      api
        .getDiagnostics()
        .then((d) => {
          setDiag(d)
          setError(null)
        })
        .catch((err) => setError(String(err instanceof Error ? err.message : err)))
    }
    load()
    const interval = setInterval(load, 3000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>Diagnostics</h2>
      <p style={styles.muted}>Aggregate Modbus counters since the gateway started.</p>
      {error && <div style={styles.errorBox}>{error}</div>}

      {!diag ? (
        !error && <p style={styles.muted}>Loading…</p>
      ) : (
        <div style={styles.cardGrid}>
          <div style={styles.card}>
            <div style={styles.cardIcon}><Icon name="sending" /></div>
            <div style={styles.cardTitle}>Modbus TX</div>
            <div style={styles.cardValue}>{fmtNum(diag.modbus_tx)}</div>
          </div>
          <div style={styles.card}>
            <div style={styles.cardIcon}><Icon name="sending" /></div>
            <div style={styles.cardTitle}>Modbus RX</div>
            <div style={styles.cardValue}>{fmtNum(diag.modbus_rx)}</div>
          </div>
          <div style={styles.card}>
            <div style={styles.cardIcon}><Icon name="activity" /></div>
            <div style={styles.cardTitle}>Avg Response Time</div>
            <div style={styles.cardValue}>{diag.avg_response_time_ms.toFixed(1)} ms</div>
          </div>
          <div style={styles.card}>
            <div style={styles.cardIcon}><Icon name="timeout" /></div>
            <div style={styles.cardTitle}>Timeouts</div>
            <div style={styles.cardValue}>{fmtNum(diag.timeout_count)}</div>
          </div>
          <div style={styles.card}>
            <div style={styles.cardIcon}><Icon name="crc" /></div>
            <div style={styles.cardTitle}>CRC Errors</div>
            <div style={styles.cardValue}>{fmtNum(diag.crc_error_count)}</div>
          </div>
          <div style={styles.card}>
            <div style={styles.cardIcon}><Icon name="retry" /></div>
            <div style={styles.cardTitle}>Retry Count</div>
            <div style={styles.cardValue}>{fmtNum(diag.retry_count)}</div>
          </div>
          <div style={styles.card}>
            <div style={styles.cardIcon}><Icon name="queue" /></div>
            <div style={styles.cardTitle}>Queue Write Rate</div>
            <div style={styles.cardValue}>{diag.write_rate_per_sec.toFixed(1)} readings/s</div>
          </div>
        </div>
      )}
    </div>
  )
}
