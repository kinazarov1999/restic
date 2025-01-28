package fs

import (
	"io"

	"github.com/restic/restic/internal/debug"
)

type CustomReader struct {
	Name string

	nameRead bool
}

func (cr *CustomReader) Read(p []byte) (int, error) {
	if cr.nameRead {
		debug.Log("<<<<<EOF!")
		return 0, io.EOF
	}

	debug.Log("<<<<<Starting file read.")

	cr.nameRead = true
	n := copy(p, cr.Name)
	debug.Log("<<<<<Ended file read.")

	return n, nil
}

func (cr *CustomReader) Close() error {
	return nil
}
