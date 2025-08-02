package core

import (
	"fmt"
	"io"
	"os"
)

// CopyFile copies a file from src to dst.
// This is an exported version used by non-test code like snapshot restore.
func CopyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat source file %s for copying: %w", src, err)
	}
	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s for copying: %w", src, err)
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s for copying: %w", dst, err)
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	if err != nil {
		return fmt.Errorf("failed to copy data from %s to %s: %w", src, dst, err)
	}
	return nil
}

func NewSimpleDataPoint(metric string, tags map[string]string, ts int64, fields map[string]any) (*DataPoint, error) {
	dp, err := NewDataPoint(
		metric,
		tags,
		ts,
	)

	if err != nil {
		return nil, err
	}

	if len(fields) > 0 {
		for k, v := range fields {
			val, err := NewPointValue(v)
			if err != nil {
				return nil, err
			}
			dp.AddField(k, val)
		}
	}

	return dp, nil
}
