package queue

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

// chunkFormatV1 is the first byte of every stored chunk blob. The rest of
// the blob is a raw DEFLATE stream of the record list below. Bump it (and
// keep a decoder for the old value) if the record layout ever changes —
// chunks written by an older binary may still be in queue.db.
const chunkFormatV1 byte = 1

// Record list layout (before compression):
//
//	uvarint  count
//	varint   base timestamp, unix ms
//	count x record:
//	  uvarint  datapoint_id
//	  uvarint  device_id
//	  varint   timestamp delta in ms from the previous record (the base for the first)
//	  byte     quality code; bit 7 set = a float64 value follows
//	  [string  uvarint length + bytes, only when the code is qualityOther]
//	  [8 bytes float64, little endian, only when bit 7 is set]
//
// The delta is signed because readings from different connections are
// appended by concurrent pollers and are only roughly in time order.

const (
	qualityGood          byte = 0
	qualityTimeout       byte = 1
	qualityCRCError      byte = 2
	qualityDeviceOffline byte = 3
	qualityInvalid       byte = 4
	qualityOther         byte = 0x7f
	hasValueBit          byte = 0x80
)

var qualityCodes = map[string]byte{
	"GOOD":           qualityGood,
	"TIMEOUT":        qualityTimeout,
	"CRC_ERROR":      qualityCRCError,
	"DEVICE_OFFLINE": qualityDeviceOffline,
	"INVALID":        qualityInvalid,
}

var qualityNames = map[byte]string{
	qualityGood:          "GOOD",
	qualityTimeout:       "TIMEOUT",
	qualityCRCError:      "CRC_ERROR",
	qualityDeviceOffline: "DEVICE_OFFLINE",
	qualityInvalid:       "INVALID",
}

// encodeChunk serializes readings into a compressed chunk blob.
func encodeChunk(readings []Reading) ([]byte, error) {
	var raw bytes.Buffer
	var tmp [binary.MaxVarintLen64]byte
	putU := func(v uint64) { raw.Write(tmp[:binary.PutUvarint(tmp[:], v)]) }
	putS := func(v int64) { raw.Write(tmp[:binary.PutVarint(tmp[:], v)]) }

	putU(uint64(len(readings)))
	var prev int64
	if len(readings) > 0 {
		prev = readings[0].At.UnixMilli()
	}
	putS(prev)

	for _, r := range readings {
		if r.DatapointID < 0 || r.DeviceID < 0 {
			return nil, fmt.Errorf("negative id in reading (device %d, datapoint %d)", r.DeviceID, r.DatapointID)
		}
		putU(uint64(r.DatapointID))
		putU(uint64(r.DeviceID))
		ts := r.At.UnixMilli()
		putS(ts - prev)
		prev = ts

		code, known := qualityCodes[r.Quality]
		if !known {
			code = qualityOther
		}
		if r.Value != nil {
			code |= hasValueBit
		}
		raw.WriteByte(code)
		if !known {
			putU(uint64(len(r.Quality)))
			raw.WriteString(r.Quality)
		}
		if r.Value != nil {
			var f [8]byte
			binary.LittleEndian.PutUint64(f[:], math.Float64bits(*r.Value))
			raw.Write(f[:])
		}
	}

	var out bytes.Buffer
	out.WriteByte(chunkFormatV1)
	zw, err := flate.NewWriter(&out, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(raw.Bytes()); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// decodeChunk is the inverse of encodeChunk.
func decodeChunk(blob []byte) ([]Reading, error) {
	if len(blob) == 0 {
		return nil, errors.New("empty chunk")
	}
	if blob[0] != chunkFormatV1 {
		return nil, fmt.Errorf("unknown chunk format %d", blob[0])
	}
	raw, err := io.ReadAll(flate.NewReader(bytes.NewReader(blob[1:])))
	if err != nil {
		return nil, fmt.Errorf("decompress chunk: %w", err)
	}
	r := bytes.NewReader(raw)

	count, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read count: %w", err)
	}
	if count > uint64(len(raw)) { // every record is at least one byte
		return nil, fmt.Errorf("corrupt chunk: count %d exceeds size", count)
	}
	prev, err := binary.ReadVarint(r)
	if err != nil {
		return nil, fmt.Errorf("read base timestamp: %w", err)
	}

	readings := make([]Reading, 0, count)
	for i := uint64(0); i < count; i++ {
		dp, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("record %d datapoint: %w", i, err)
		}
		dev, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("record %d device: %w", i, err)
		}
		delta, err := binary.ReadVarint(r)
		if err != nil {
			return nil, fmt.Errorf("record %d timestamp: %w", i, err)
		}
		prev += delta
		code, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("record %d quality: %w", i, err)
		}

		rd := Reading{DatapointID: int64(dp), DeviceID: int64(dev), At: time.UnixMilli(prev).UTC()}
		base := code &^ hasValueBit
		if base == qualityOther {
			n, err := binary.ReadUvarint(r)
			if err != nil || n > uint64(r.Len()) {
				return nil, fmt.Errorf("record %d quality string: bad length", i)
			}
			s := make([]byte, n)
			if _, err := io.ReadFull(r, s); err != nil {
				return nil, fmt.Errorf("record %d quality string: %w", i, err)
			}
			rd.Quality = string(s)
		} else if name, ok := qualityNames[base]; ok {
			rd.Quality = name
		} else {
			return nil, fmt.Errorf("record %d: unknown quality code %d", i, base)
		}
		if code&hasValueBit != 0 {
			var f [8]byte
			if _, err := io.ReadFull(r, f[:]); err != nil {
				return nil, fmt.Errorf("record %d value: %w", i, err)
			}
			v := math.Float64frombits(binary.LittleEndian.Uint64(f[:]))
			rd.Value = &v
		}
		readings = append(readings, rd)
	}
	return readings, nil
}
