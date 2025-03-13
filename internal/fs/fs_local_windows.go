package fs

import (
	"github.com/restic/restic/internal/errors"
)

func (f *localFile) GetBlockDeviceSize() (uint64, error) {
	return nil, errors.New("Backup of block devices is not supported on Windows")
}
