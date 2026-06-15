package dump

import (
	"time"

	"github.com/restic/restic/internal/ui"
)

type jsonPrinter struct {
	terminal ui.Terminal
}

func NewJSONProgress(terminal ui.Terminal) Printer {
	return &jsonPrinter{
		terminal: terminal,
	}
}

func (p *jsonPrinter) print(status interface{}) {
	p.terminal.Error(ui.ToJSONString(status))
}

func (p *jsonPrinter) Update(bytesWritten, bytesTotal uint64, duration time.Duration, final bool) {
	var pct float64
	if bytesTotal > 0 {
		pct = float64(bytesWritten) / float64(bytesTotal)
	}

	msgType := "status"
	if final {
		msgType = "summary"
	}

	status := statusUpdate{
		MessageType:    msgType,
		SecondsElapsed: uint64(duration / time.Second),
		PercentDone:    pct,
		BytesWritten:   bytesWritten,
		BytesTotal:     bytesTotal,
	}

	p.print(status)
}

type statusUpdate struct {
	MessageType    string  `json:"message_type"`
	SecondsElapsed uint64  `json:"seconds_elapsed"`
	PercentDone    float64 `json:"percent_done"`
	BytesWritten   uint64  `json:"bytes_written"`
	BytesTotal     uint64  `json:"bytes_total"`
}
