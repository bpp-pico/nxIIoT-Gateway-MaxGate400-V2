package api

import "net/http"

// diagnosticsDTO matches §16's Diagnostics panel fields.
type diagnosticsDTO struct {
	ModbusTX          int64   `json:"modbus_tx"`
	ModbusRX          int64   `json:"modbus_rx"`
	AvgResponseTimeMs float64 `json:"avg_response_time_ms"`
	TimeoutCount      int64   `json:"timeout_count"`
	CRCErrorCount     int64   `json:"crc_error_count"`
	RetryCount        int64   `json:"retry_count"`
	// WriteRatePerSec is the live data_queue insert rate (rows/sec), same
	// metric as storeForwardStatusDTO's — surfaced here too so operators
	// can compare it against eviction capacity while looking at Modbus
	// throughput, without switching pages.
	WriteRatePerSec float64 `json:"write_rate_per_sec"`
}

func (s *Server) getDiagnostics(w http.ResponseWriter, r *http.Request) {
	dto := diagnosticsDTO{}
	if s.diag != nil {
		snap := s.diag.Snapshot()
		dto.ModbusTX = snap.TXCount
		dto.ModbusRX = snap.RXCount
		dto.AvgResponseTimeMs = snap.AvgResponseTimeMs
		dto.TimeoutCount = snap.TimeoutCount
		dto.CRCErrorCount = snap.CRCErrorCount
		dto.RetryCount = snap.RetryCount
	}
	dto.WriteRatePerSec = s.queueWriteRatePerSec()
	writeJSON(w, http.StatusOK, dto)
}
