// The MIT License
//
// Copyright (c) 2025 Peter A McGill
package dtype

import (
	"github.com/pcbuildpluscoding/logroll"
)

var (
	logger logroll.Logger
)

// ===================================================================
func SetLogger(super *logroll.LogFile) {
	logger = super
}

// ===================================================================
func NewFrame(b []byte) *frame {
	return &frame{B: b}
}

// ===================================================================
func NewHdpError(code int, args ...any) *HdpError {
	return &HdpError{
		code: code,
		args: args,
	}
}

// ===================================================================
func NewHdpEvent(args ...any) HdpEvent {
	ev := HdpEvent{}
	return ev.With(args...)
}
