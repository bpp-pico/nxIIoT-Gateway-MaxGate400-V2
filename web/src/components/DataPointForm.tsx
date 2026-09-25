import { useState } from 'react'
import type { DataPoint, DataType } from '../types'
import { styles } from '../styles'

interface DataPointFormProps {
  initial?: DataPoint
  onSubmit: (dp: Partial<DataPoint>) => Promise<void>
  onCancel: () => void
}

const emptyForm = {
  tag_name: '',
  function_code: 3,
  register_address: 0,
  data_type: 'INT16' as DataType,
  byte_order: '',
  word_order: '',
  scale: 1,
  offset: 0,
  unit: '',
  enabled: true,
}

const dataTypes: DataType[] = ['INT16', 'UINT16', 'INT32', 'UINT32', 'FLOAT32', 'FLOAT64']
const is32bit = (dt: DataType) => dt === 'INT32' || dt === 'UINT32' || dt === 'FLOAT32'
const is64bit = (dt: DataType) => dt === 'FLOAT64'

export function DataPointForm({ initial, onSubmit, onCancel }: DataPointFormProps) {
  const [form, setForm] = useState({ ...emptyForm, ...initial })
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  const set = <K extends keyof typeof form>(key: K, value: (typeof form)[K]) =>
    setForm((f) => ({ ...f, [key]: value }))

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setSaving(true)
    setError(null)
    try {
      await onSubmit(form)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    } finally {
      setSaving(false)
    }
  }

  const showByteOrder = is32bit(form.data_type) || is64bit(form.data_type)

  return (
    <form onSubmit={handleSubmit}>
      {error && <div style={styles.errorBox}>{error}</div>}

      <div style={styles.formRow}>
        <label style={styles.label}>Tag Name</label>
        <input style={styles.input} value={form.tag_name} onChange={(e) => set('tag_name', e.target.value)} required />
      </div>

      <div style={{ display: 'flex', gap: '0.75rem' }}>
        <div style={{ ...styles.formRow, flex: 1 }}>
          <label style={styles.label}>Function Code</label>
          <select
            style={styles.input}
            value={form.function_code}
            onChange={(e) => set('function_code', Number(e.target.value))}
          >
            <option value={1}>01 - Read Coils</option>
            <option value={2}>02 - Read Discrete Inputs</option>
            <option value={3}>03 - Read Holding Registers</option>
            <option value={4}>04 - Read Input Registers</option>
          </select>
        </div>
        <div style={{ ...styles.formRow, flex: 1 }}>
          <label style={styles.label}>Register Address</label>
          <input
            type="number"
            style={styles.input}
            value={form.register_address}
            onChange={(e) => set('register_address', Number(e.target.value))}
          />
        </div>
      </div>

      <div style={{ display: 'flex', gap: '0.75rem' }}>
        <div style={{ ...styles.formRow, flex: 1 }}>
          <label style={styles.label}>Data Type</label>
          <select style={styles.input} value={form.data_type} onChange={(e) => set('data_type', e.target.value as DataType)}>
            {dataTypes.map((dt) => (
              <option key={dt} value={dt}>
                {dt}
              </option>
            ))}
          </select>
        </div>
        {showByteOrder && (
          <div style={{ ...styles.formRow, flex: 1 }}>
            <label style={styles.label}>Byte Order</label>
            <select style={styles.input} value={form.byte_order} onChange={(e) => set('byte_order', e.target.value)}>
              {is64bit(form.data_type) ? (
                <>
                  <option value="">Natural (ABCDEFGH)</option>
                  <option value="HGFEDCBA">Reversed (HGFEDCBA)</option>
                </>
              ) : (
                <>
                  <option value="ABCD">ABCD</option>
                  <option value="BADC">BADC</option>
                  <option value="CDAB">CDAB</option>
                  <option value="DCBA">DCBA</option>
                </>
              )}
            </select>
          </div>
        )}
      </div>

      <div style={{ display: 'flex', gap: '0.75rem' }}>
        <div style={{ ...styles.formRow, flex: 1 }}>
          <label style={styles.label}>Scale</label>
          <input
            type="number"
            step="any"
            style={styles.input}
            value={form.scale}
            onChange={(e) => set('scale', Number(e.target.value))}
          />
        </div>
        <div style={{ ...styles.formRow, flex: 1 }}>
          <label style={styles.label}>Offset</label>
          <input
            type="number"
            step="any"
            style={styles.input}
            value={form.offset}
            onChange={(e) => set('offset', Number(e.target.value))}
          />
        </div>
      </div>

      <div style={{ display: 'flex', gap: '0.75rem' }}>
        <div style={{ ...styles.formRow, flex: 1 }}>
          <label style={styles.label}>Unit</label>
          <input style={styles.input} value={form.unit} onChange={(e) => set('unit', e.target.value)} />
        </div>
      </div>

      <div style={{ ...styles.formRow, flexDirection: 'row', alignItems: 'center', gap: '0.5rem' }}>
        <input
          type="checkbox"
          id="datapoint-enabled"
          checked={form.enabled}
          onChange={(e) => set('enabled', e.target.checked)}
        />
        <label htmlFor="datapoint-enabled" style={styles.label}>
          Enabled
        </label>
      </div>

      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.5rem', marginTop: '1rem' }}>
        <button type="button" style={styles.button} onClick={onCancel} disabled={saving}>
          Cancel
        </button>
        <button type="submit" style={styles.primaryButton} disabled={saving}>
          {saving ? 'Saving…' : 'Save'}
        </button>
      </div>
    </form>
  )
}
