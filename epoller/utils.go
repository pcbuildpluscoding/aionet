package epoller

import (
	"fmt"
	"io"

	"github.com/pcbuildpluscoding/aionet/dtype"
)

// -------------------------------------------------------------- //
// NewHdpError
// ---------------------------------------------------------------//
func NewHdpError(code int, args ...any) *HdpError {
	return &HdpError{
		code: code,
		args: args,
	}
}

// -------------------------------------------------------------- //
// NewHdpError
// ---------------------------------------------------------------//
func NewHdpError1(code int, flag dtype.HDP_STATE1, args ...any) *HdpError {
	return &HdpError{
		code: code,
		flag: flag,
		args: args,
	}
}

// ==================================================================//
// HDPError
// ==================================================================//
type HdpError struct {
	code int
	flag dtype.HDP_STATE1
	args []any
}

// ==================================================================
func (e *HdpError) Error() string {
	switch e.code {
	case 1:
		return "connection peer refnum does not match locally"
	case 2:
		return fmt.Sprintf("%s %s readFrom[%d] new conn error : %v", e.args...)
	case 3:
		return fmt.Sprintf("%s %s checksum verification failed : %v", e.args...)
	case 4:
		return fmt.Sprintf("%s %s read %s flag is required", e.args...)
	case 5:
		return fmt.Sprintf("%s %s connect to new conn error : %v", e.args...)
	case 6:
		return fmt.Sprintf("%s %s read header error : %v", e.args...)
	case 7:
		return fmt.Sprintf("%s %s write header error : %v", e.args...)
	case 8:
		return fmt.Sprintf("%s %s writeTo[%d] new conn error : %v", e.args...)
	case 9:
		return fmt.Sprintf("%s unsupported flag : %s", e.args...)
	case 10:
		return fmt.Sprintf("%s uint32 seqnum overflow", e.args...)
	case 11:
		return fmt.Sprintf("%s can't move from %s to %s", e.args...)
	case 12:
		return fmt.Sprintf("%s ring buffer is full", e.args...)
	case 13:
		return fmt.Sprintf("%s io request is cancelled", e.args...)
	case 14:
		return fmt.Sprintf("%s io request timed-out", e.args...)
	case 15:
		return fmt.Sprintf("%s connection is closing", e.args...)
	case 16:
		return fmt.Sprintf("%s connection is closed", e.args...)
	case 17:
		return fmt.Sprintf("%s connection is aborted", e.args...)
	case 18:
		return fmt.Sprintf("%s connection is refused", e.args...)
	case 19:
		return fmt.Sprintf("%s connection is reset", e.args...)
	case 20:
		return fmt.Sprintf("%s closing error : %v", e.args...)
	case 21:
		return fmt.Sprintf("%s dialing remote %s timed-out", e.args...)
	case 22:
		return fmt.Sprintf("%s connect protocol error %v", e.args...)
	case 23:
		return fmt.Sprintf("%s socket opt error query timeout", e.args...)
	case 24:
		return fmt.Sprintf("%s socket create error : %v", e.args...)
	case 25:
		return fmt.Sprintf("%s socket bind error : %v", e.args...)
	case 26:
		return fmt.Sprintf("%s socket state is EOF", e.args...)
	}
	return fmt.Sprintf("unsupported error code : %d", e.code)
}

// ==================================================================
func (e *HdpError) Is(err error) bool {
	switch ptr := err.(type) {
	case *HdpError:
		return ptr.code == e.code
	}
	return false
}

// ==================================================================
func (e *HdpError) EOF() error {
	if e.code == 26 {
		return io.EOF
	}
	return e
}

// ==================================================================
func formatText(s string, shorten bool) string {
	if shorten && len(s) > 16 {
		s = s[:16] + "..."
	}
	return s
}
