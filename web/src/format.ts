// Thousands-separated integer formatting for counters/durations shown in
// the UI (pending records, retry counts, poll durations, log attrs, etc.)
// — plain numbers like 218920 are hard to read at a glance once a queue
// backlog or a TX/RX counter climbs into the tens/hundreds of thousands.
export function fmtNum(v: number): string {
  return v.toLocaleString('en-US')
}

// Binary-unit byte size (1 KB = 1024 B), e.g. the queue file size.
export function fmtBytes(v?: number): string {
  if (v === undefined) return '—'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let n = v
  let i = 0
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024
    i++
  }
  return `${n.toFixed(1)} ${units[i]}`
}
