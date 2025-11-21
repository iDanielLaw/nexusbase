//go:build !unix && !windows
// +build !unix,!windows

package sys

func NewFile() File {
	return nil
}
