package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"nxiiot-gateway/internal/datapoint"
)

// dataPointDTO is the wire representation of a Data Point, per §6 Data Point Model.
// LastValue/LastQuality/LastReadAt reflect the acquisition engine's live
// polling (internal/acquisition.LatestStore), not the on-demand Test Read —
// they're omitted entirely until the data point has been polled at least
// once since gateway startup (e.g. it's disabled, or acquisition hasn't
// reached it yet).
type dataPointDTO struct {
	ID              int64      `json:"id"`
	DeviceID        int64      `json:"device_id"`
	TagName         string     `json:"tag_name"`
	FunctionCode    uint8      `json:"function_code"`
	RegisterAddress uint16     `json:"register_address"`
	DataType        string     `json:"data_type"`
	ByteOrder       string     `json:"byte_order,omitempty"`
	WordOrder       string     `json:"word_order,omitempty"`
	Scale           float64    `json:"scale"`
	Offset          float64    `json:"offset"`
	Unit            string     `json:"unit,omitempty"`
	Enabled         bool       `json:"enabled"`
	LastValue       *float64   `json:"last_value,omitempty"`
	LastQuality     string     `json:"last_quality,omitempty"`
	LastReadAt      *time.Time `json:"last_read_at,omitempty"`
}

func toDataPointDTO(dp datapoint.DataPoint) dataPointDTO {
	return dataPointDTO{
		ID:              dp.ID,
		DeviceID:        dp.DeviceID,
		TagName:         dp.TagName,
		FunctionCode:    dp.FunctionCode,
		RegisterAddress: dp.RegisterAddress,
		DataType:        dp.DataType,
		ByteOrder:       dp.ByteOrder,
		WordOrder:       dp.WordOrder,
		Scale:           dp.Scale,
		Offset:          dp.Offset,
		Unit:            dp.Unit,
		Enabled:         dp.Enabled,
	}
}

// withLatest fills in the live-polling fields from the acquisition engine's
// LatestStore, leaving them unset if this data point hasn't been polled yet
// (s.latest is nil in tests that don't wire one up, or the point simply
// hasn't come up for a read since gateway startup).
func (s *Server) withLatest(dto dataPointDTO) dataPointDTO {
	if s.latest == nil {
		return dto
	}
	lv, ok := s.latest.Get(dto.ID)
	if !ok {
		return dto
	}
	dto.LastValue = lv.Value
	dto.LastQuality = lv.Quality
	at := lv.At
	dto.LastReadAt = &at
	return dto
}

func (dto dataPointDTO) toDataPoint() datapoint.DataPoint {
	return datapoint.DataPoint{
		ID:              dto.ID,
		DeviceID:        dto.DeviceID,
		TagName:         dto.TagName,
		FunctionCode:    dto.FunctionCode,
		RegisterAddress: dto.RegisterAddress,
		DataType:        dto.DataType,
		ByteOrder:       dto.ByteOrder,
		WordOrder:       dto.WordOrder,
		Scale:           dto.Scale,
		Offset:          dto.Offset,
		Unit:            dto.Unit,
		Enabled:         dto.Enabled,
	}
}

func (s *Server) listDataPoints(w http.ResponseWriter, r *http.Request) {
	deviceID, err := parseID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	points, err := s.datapointRepo.ListByDevice(r.Context(), deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := make([]dataPointDTO, len(points))
	for i, dp := range points {
		out[i] = s.withLatest(toDataPointDTO(dp))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) createDataPoint(w http.ResponseWriter, r *http.Request) {
	deviceID, err := parseID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.deviceRepo.Get(r.Context(), deviceID); err != nil {
		writeDeviceRepoError(w, err)
		return
	}

	var dto dataPointDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	dp := dto.toDataPoint()
	dp.DeviceID = deviceID
	if err := dp.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	id, err := s.datapointRepo.Create(r.Context(), dp)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	dp.ID = id

	s.reload(r)
	writeJSON(w, http.StatusCreated, toDataPointDTO(dp))
}

func (s *Server) updateDataPoint(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	existing, err := s.datapointRepo.Get(r.Context(), id)
	if err != nil {
		writeDataPointRepoError(w, err)
		return
	}

	var dto dataPointDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	dp := dto.toDataPoint()
	dp.ID = id
	dp.DeviceID = existing.DeviceID // device_id is immutable via this endpoint
	if err := dp.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.datapointRepo.Update(r.Context(), id, dp); err != nil {
		writeDataPointRepoError(w, err)
		return
	}

	s.reload(r)
	writeJSON(w, http.StatusOK, toDataPointDTO(dp))
}

func (s *Server) deleteDataPoint(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.datapointRepo.Delete(r.Context(), id); err != nil {
		writeDataPointRepoError(w, err)
		return
	}

	s.reload(r)
	w.WriteHeader(http.StatusNoContent)
}

func writeDataPointRepoError(w http.ResponseWriter, err error) {
	if errors.Is(err, datapoint.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}
