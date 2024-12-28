package fs

import (
	"errors"
	"io"
	"os"
)

type PartialFile struct {
	File File

	base  int64
	off   int64
	limit int64
}

var errWhence = errors.New("Seek: invalid whence")
var errOffset = errors.New("Seek: invalid offset")

func NewPartialFile(f File, startByte int64, endByte int64) *PartialFile {
	return &PartialFile{f, startByte, 0, endByte}
}

func (pf *PartialFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	default:
		return 0, errWhence
	case io.SeekStart:
		offset += pf.base
	case io.SeekCurrent:
		offset += pf.off
	case io.SeekEnd:
		offset += pf.limit
	}
	if offset < pf.base {
		return 0, errOffset
	}
	pf.off = offset
	return offset - pf.base, nil
}

func (pf *PartialFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= pf.limit-pf.base {
		return 0, io.EOF
	}
	off += pf.base
	if max := pf.limit - off; int64(len(p)) > max {
		p = p[0:max]
		n, err = pf.File.ReadAt(p, off)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return pf.File.ReadAt(p, off)
}

func (pf *PartialFile) Read(p []byte) (n int, err error) {
	if pf.off >= pf.limit {
		return 0, io.EOF
	}
	if max := pf.limit - pf.off; int64(len(p)) > max {
		p = p[0:max]
	}
	n, err = pf.ReadAt(p, pf.off)
	pf.off += int64(n)
	return
}

func (pf *PartialFile) Close() error {
	return pf.File.Close()
}

func (pf *PartialFile) Fd() uintptr {
	return pf.File.Fd()
}

func (pf *PartialFile) Readdirnames(n int) ([]string, error) {
	return pf.File.Readdirnames(n)
}

func (pf *PartialFile) Readdir(n int) ([]os.FileInfo, error) {
	return pf.File.Readdir(n)
}

func (pf *PartialFile) Stat() (os.FileInfo, error) {
	return pf.File.Stat()
}
