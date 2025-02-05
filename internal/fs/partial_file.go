package fs

import (
	"io"
)

type PartialFile struct {
	File File

	base  int64
	off   int64
	limit int64
}

func NewPartialFile(f File, startByte int64, endByte int64) *PartialFile {
	return &PartialFile{f, startByte, 0, endByte}
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
