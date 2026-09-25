// Package datapoint provides the Data Point configuration model and repository.
package datapoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"nxiiot-gateway/internal/modbus"
)

var ErrNotFound = errors.New("datapoint not found")

type DataPoint struct {
	ID              int64
	DeviceID        int64
	TagName         string
	FunctionCode    uint8
	RegisterAddress uint16
	DataType        string
	ByteOrder       string
	WordOrder       string
	Scale           float64
	Offset          float64
	Unit            string
	Enabled         bool
}

// Validate checks the fields required by the Data Point Model (design doc §6).
func (dp DataPoint) Validate() error {
	if dp.TagName == "" {
		return fmt.Errorf("tag_name is required")
	}
	switch dp.FunctionCode {
	case 1, 2, 3, 4:
	default:
		return fmt.Errorf("function_code must be one of 1, 2, 3, 4")
	}
	if _, err := modbus.DataType(dp.DataType).ByteWidth(); err != nil {
		return fmt.Errorf("data_type: %w", err)
	}
	if dp.Scale == 0 {
		return fmt.Errorf("scale must not be zero")
	}
	return nil
}

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

const selectColumns = `id, device_id, tag_name, function_code, register_address, data_type,
	byte_order, word_order, scale, offset, unit, enabled`

func scanDataPoint(row interface{ Scan(...any) error }) (DataPoint, error) {
	var dp DataPoint
	var unit sql.NullString
	var enabled int
	err := row.Scan(&dp.ID, &dp.DeviceID, &dp.TagName, &dp.FunctionCode, &dp.RegisterAddress,
		&dp.DataType, &dp.ByteOrder, &dp.WordOrder, &dp.Scale, &dp.Offset, &unit,
		&enabled)
	if err != nil {
		return DataPoint{}, err
	}
	dp.Unit = unit.String
	dp.Enabled = enabled != 0
	return dp, nil
}

// ListByDevice returns every data point for a device, enabled or not.
func (r *Repository) ListByDevice(ctx context.Context, deviceID int64) ([]DataPoint, error) {
	return r.query(ctx, `SELECT `+selectColumns+` FROM datapoint WHERE device_id = ? ORDER BY id`, deviceID)
}

// ListEnabledByDevice returns all enabled data points for a device.
func (r *Repository) ListEnabledByDevice(ctx context.Context, deviceID int64) ([]DataPoint, error) {
	return r.query(ctx, `SELECT `+selectColumns+` FROM datapoint WHERE device_id = ? AND enabled = 1 ORDER BY id`, deviceID)
}

func (r *Repository) query(ctx context.Context, query string, args ...any) ([]DataPoint, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var points []DataPoint
	for rows.Next() {
		dp, err := scanDataPoint(rows)
		if err != nil {
			return nil, err
		}
		points = append(points, dp)
	}
	return points, rows.Err()
}

// CountAll returns the total number of data points across every device —
// for the Dashboard "Data Point Count" widget (§16).
func (r *Repository) CountAll(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM datapoint`).Scan(&count)
	return count, err
}

// Get returns a single data point by id, or ErrNotFound.
func (r *Repository) Get(ctx context.Context, id int64) (DataPoint, error) {
	row := r.db.QueryRowContext(ctx, `SELECT `+selectColumns+` FROM datapoint WHERE id = ?`, id)
	dp, err := scanDataPoint(row)
	if errors.Is(err, sql.ErrNoRows) {
		return DataPoint{}, ErrNotFound
	}
	if err != nil {
		return DataPoint{}, err
	}
	return dp, nil
}

// Create inserts a new data point and returns its id.
func (r *Repository) Create(ctx context.Context, dp DataPoint) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
		INSERT INTO datapoint (device_id, tag_name, function_code, register_address, data_type,
		                        byte_order, word_order, scale, offset, unit, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dp.DeviceID, dp.TagName, dp.FunctionCode, dp.RegisterAddress, dp.DataType,
		dp.ByteOrder, dp.WordOrder, dp.Scale, dp.Offset, nullable(dp.Unit),
		boolToInt(dp.Enabled))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Update replaces all editable fields of an existing data point.
func (r *Repository) Update(ctx context.Context, id int64, dp DataPoint) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE datapoint SET
			tag_name = ?, function_code = ?, register_address = ?, data_type = ?,
			byte_order = ?, word_order = ?, scale = ?, offset = ?, unit = ?,
			enabled = ?,
			updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		WHERE id = ?`,
		dp.TagName, dp.FunctionCode, dp.RegisterAddress, dp.DataType,
		dp.ByteOrder, dp.WordOrder, dp.Scale, dp.Offset, nullable(dp.Unit),
		boolToInt(dp.Enabled), id)
	if err != nil {
		return err
	}
	return checkRowsAffected(res)
}

// Delete removes a data point.
func (r *Repository) Delete(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM datapoint WHERE id = ?`, id)
	if err != nil {
		return err
	}
	return checkRowsAffected(res)
}

func checkRowsAffected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
