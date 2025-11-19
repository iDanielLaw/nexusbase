package sys

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

var _ FileHandle = (*DebugFile)(nil)
var nextID atomic.Uint64

var listFD *sync.Map = new(sync.Map)

type DebugFile struct {
	id     uint64
	f      *os.File
	logger *slog.Logger
}

func DCreate(sysFile File, name string) (FileHandle, error) {
	return DOpenFile(sysFile, name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func DOpen(sysFile File, name string) (FileHandle, error) {
	return DOpenFile(sysFile, name, os.O_RDONLY, 0)
}

func DOpenFile(sysFile File, name string, flag int, perm os.FileMode) (FileHandle, error) {
	// Assume a default logger if none is provided, or you could pass it in.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).With("component", "DebugFile")

	f, err := sysFile.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	// Atomically increment and use the returned value as the ID.
	id := nextID.Add(1)
	logger = logger.With("id", id)
	logger = logger.With("file_name", name)
	logger.Debug("Opening file")
	listFD.Store(id, f.Name())
	// logger.Debug(string(debug.Stack()))

	return &DebugFile{
		id:     id,
		f:      f,
		logger: logger,
	}, nil
}

func (df *DebugFile) Write(p []byte) (n int, err error) {
	return df.f.Write(p)
}

func (df *DebugFile) Read(p []byte) (n int, err error) {
	return df.f.Read(p)
}

func (df *DebugFile) Seek(offset int64, whence int) (int64, error) {
	return df.f.Seek(offset, whence)
}

func (df *DebugFile) Stat() (os.FileInfo, error) {
	return df.f.Stat()
}

func (df *DebugFile) Sync() error {
	return df.f.Sync()
}

func (df *DebugFile) Truncate(size int64) error {
	return df.f.Truncate(size)
}

func (df *DebugFile) Name() string {
	return df.f.Name()
}

func (df *DebugFile) WriteAt(p []byte, off int64) (n int, err error) {
	return df.f.WriteAt(p, off)
}

func (df *DebugFile) ReadAt(p []byte, off int64) (n int, err error) {
	return df.f.ReadAt(p, off)
}

func (df *DebugFile) WriteString(s string) (n int, err error) {
	return df.f.WriteString(s)
}

func (df *DebugFile) WriteTo(w io.Writer) (n int64, err error) {
	return df.f.WriteTo(w)
}

func (df *DebugFile) ReadFrom(r io.Reader) (n int64, err error) {
	return df.f.ReadFrom(r)
}

func (df *DebugFile) Close() error {
	df.logger.Debug("Closing file")
	listFD.Delete(df.id)
	// df.logger.Debug(string(debug.Stack()))
	return df.f.Close()
}

func PrintMapFiles() {
	fmt.Println("List Files Opening")
	listFD.Range(func(key, value any) bool {
		fmt.Printf("Key: %v, Value: %v\n", key, value)
		return true
	})
}
