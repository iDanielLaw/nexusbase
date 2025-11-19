package sys

import (
	"io"
	"os"
)

var _ FileHandle = (*RealFile)(nil)

type RealFile struct {
	f *os.File
}

func RCreate(sysFile File, name string) (FileHandle, error) {
	return ROpenFile(sysFile, name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func ROpen(sysFile File, name string) (FileHandle, error) {
	return ROpenFile(sysFile, name, os.O_RDONLY, 0)
}

func ROpenFile(sysFile File, name string, flag int, perm os.FileMode) (FileHandle, error) {
	f, err := sysFile.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return &RealFile{
		f: f,
	}, nil
}

func (df *RealFile) Write(p []byte) (n int, err error) {
	return df.f.Write(p)
}

func (df *RealFile) Read(p []byte) (n int, err error) {
	return df.f.Read(p)
}

func (df *RealFile) Seek(offset int64, whence int) (int64, error) {
	return df.f.Seek(offset, whence)
}

func (df *RealFile) Stat() (os.FileInfo, error) {
	return df.f.Stat()
}

func (df *RealFile) Sync() error {
	return df.f.Sync()
}

func (df *RealFile) Truncate(size int64) error {
	return df.f.Truncate(size)
}

func (df *RealFile) Name() string {
	return df.f.Name()
}

func (df *RealFile) WriteAt(p []byte, off int64) (n int, err error) {
	return df.f.WriteAt(p, off)
}

func (df *RealFile) ReadAt(p []byte, off int64) (n int, err error) {
	return df.f.ReadAt(p, off)
}

func (df *RealFile) WriteString(s string) (n int, err error) {
	return df.f.WriteString(s)
}

func (df *RealFile) WriteTo(w io.Writer) (n int64, err error) {
	return df.f.WriteTo(w)
}

func (df *RealFile) ReadFrom(r io.Reader) (n int64, err error) {
	return df.f.ReadFrom(r)
}

func (df *RealFile) Close() error {
	return df.f.Close()
}
