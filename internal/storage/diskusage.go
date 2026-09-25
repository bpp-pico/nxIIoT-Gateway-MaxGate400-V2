package storage

import (
	"path/filepath"

	"github.com/shirou/gopsutil/v3/disk"
)

// DiskUsagePercent returns the used-space percentage (0-100) of the volume
// containing dbPath, for the storage threshold policy (§17). dbPath itself
// need not exist yet — only its parent directory is inspected.
func DiskUsagePercent(dbPath string) (float64, error) {
	dir := filepath.Dir(dbPath)
	usage, err := disk.Usage(dir)
	if err != nil {
		return 0, err
	}
	return usage.UsedPercent, nil
}

// DiskUsage returns the used bytes and the usable size (used + free, the
// same basis df and UsedPercent use: ext4's root-reserved blocks are left
// out) of the volume containing path. path itself need not exist yet.
func DiskUsage(path string) (used, total uint64, err error) {
	usage, err := disk.Usage(filepath.Dir(path))
	if err != nil {
		return 0, 0, err
	}
	return usage.Used, usage.Used + usage.Free, nil
}
