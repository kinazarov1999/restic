package dump

import (
	"time"
)

// Printer is the interface that progress printers must implement.
type Printer interface {
	Update(bytesWritten, bytesTotal uint64, duration time.Duration, final bool)
}
