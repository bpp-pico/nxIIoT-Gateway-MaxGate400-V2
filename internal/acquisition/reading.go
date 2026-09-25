package acquisition

import (
	"time"

	"nxiiot-gateway/internal/modbus"
)

// Reading is one acquired Data Point value. EventTimestamp is captured at
// the moment the Modbus response is received (Design Principle #6) and must
// never be changed downstream. Value is nil whenever Quality != GOOD — a
// stale value must never be reused and reported as GOOD.
type Reading struct {
	DeviceID       int64
	DeviceName     string
	DatapointID    int64
	Tag            string
	Value          *float64
	Quality        modbus.Quality
	Unit           string
	EventTimestamp time.Time
}
