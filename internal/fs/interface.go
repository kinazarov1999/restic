package fs

import (
	"io"
	"os"
)

// FS bundles all methods needed for a file system.
type FS interface {
	Open(name string) (File, error)
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Stat(name string) (os.FileInfo, error)
	Lstat(name string) (os.FileInfo, error)
	MapFilename(filename string) string

	Join(elem ...string) string
	Separator() string
	Abs(path string) (string, error)
	Clean(path string) string
	VolumeName(path string) string
	IsAbs(path string) bool

	Dir(path string) string
	Base(path string) string
}

// File is an open file on a file system.
type File interface {
	io.Reader
	io.Closer

	Fd() uintptr
	Readdirnames(n int) ([]string, error)
	Readdir(int) ([]os.FileInfo, error)
	Seek(int64, int) (int64, error)
	Stat() (os.FileInfo, error)
	ReadAt(b []byte, off int64) (n int, err error)
}
