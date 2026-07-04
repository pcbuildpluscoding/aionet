package dtype

import (
	"net"
	"time"
)

// ---------------------------------------------------------------//
// Activity
// ---------------------------------------------------------------//
func (r HdpEvent) Actvity(arg ...string) Activity {
	key := "activity"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch a := r[key].(type) {
	case int:
		return Activity(a)
	case Activity:
		return a
	}
	return Activity(0)
}

// ---------------------------------------------------------------//
// Addr
// ---------------------------------------------------------------//
func (r HdpEvent) Addr(key string) net.Addr {
	switch addr := r[key].(type) {
	case net.Addr:
		return addr
	}
	return nil
}

// ---------------------------------------------------------------//
// Bool
// ---------------------------------------------------------------//
func (r HdpEvent) Bool(key string) bool {
	switch b := r[key].(type) {
	case bool:
		return b
	}
	return false
}

// -------------------------------------------------------------- //
// Bytes
// ---------------------------------------------------------------//
func (r HdpEvent) Bytes(arg ...string) []byte {
	key := "bytes"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch b := r[key].(type) {
	case []byte:
		return b
	}
	return nil
}

// ---------------------------------------------------------------//
// Deadline
// ---------------------------------------------------------------//
func (r HdpEvent) Deadline(arg ...string) time.Duration {
	key := "setDeadline"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch dura := r[key].(type) {
	case time.Duration:
		return dura
	}
	return time.Duration(0)
}

// ---------------------------------------------------------------//
// Ch
// ---------------------------------------------------------------//
func (r HdpEvent) Ch(arg ...string) EventCh {
	key := "resultCh"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch ch := r[key].(type) {
	case chan HdpEvent:
		return EventCh(ch)
	case EventCh:
		return ch
	}
	return nil
}

// ---------------------------------------------------------------//
// Err
// ---------------------------------------------------------------//
func (r HdpEvent) Err(arg ...string) error {
	key := "error"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch e := r[key].(type) {
	case error:
		return e
	}
	return nil
}

// ---------------------------------------------------------------//
// Flag1
// ---------------------------------------------------------------//
func (r HdpEvent) Flag1(arg ...string) HDP_STATE1 {
	key := "flag1"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch f1 := r[key].(type) {
	case HDP_STATE1:
		return f1
	case int:
		return HDP_STATE1(f1)
	}
	return HDP_STATE1(0)
}

// ---------------------------------------------------------------//
// Flag1
// ---------------------------------------------------------------//
func (r HdpEvent) Flag2(arg ...string) HDP_STATE2 {
	key := "flag2"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch f2 := r[key].(type) {
	case HDP_STATE2:
		return f2
	case int:
		return HDP_STATE2(f2)
	}
	return HDP_STATE2(0)
}

// -------------------------------------------------------------- //
// Frame1
// ---------------------------------------------------------------//
func (r HdpEvent) Frame(arg ...string) *frame {
	key := "frame"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch f := r[key].(type) {
	case frame:
		return &f
	case *frame:
		return f
	}
	return nil
}

// ---------------------------------------------------------------//
// Int
// ---------------------------------------------------------------//
func (r HdpEvent) Int(key string) int {
	switch i := r[key].(type) {
	case int:
		return i
	case uint16:
		return int(i)
	case uint32:
		return int(i)
	case float32:
		return int(i)
	case float64:
		return int(i)
	}
	x, _ := r[key].(int)
	return x
}

// ---------------------------------------------------------------//
// String
// ---------------------------------------------------------------//
func (r HdpEvent) String(key string) string {
	switch s := r[key].(type) {
	case []byte:
		return string(s)
	case string:
		return s
	}
	return ""
}

// ---------------------------------------------------------------//
// Ch
// ---------------------------------------------------------------//
func (r HdpEvent) MultiCh(arg ...string) MultiCh {
	key := "multiCh"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch ch := r[key].(type) {
	case MultiCh:
		return ch
	}
	return MultiCh{}
}

// ---------------------------------------------------------------//
// UInt16
// ---------------------------------------------------------------//
func (r HdpEvent) UInt16(key string) uint16 {
	switch i := r[key].(type) {
	case int:
		return uint16(i)
	case uint16:
		return i
	}
	return 0
}

// ---------------------------------------------------------------//
// UInt32
// ---------------------------------------------------------------//
func (r HdpEvent) UInt32(key string) uint32 {
	switch i := r[key].(type) {
	case int:
		return uint32(i)
	case uint16:
		return uint32(i)
	case uint32:
		return i
	}
	return 0
}

// ==================================================================//
// EventCh
// ==================================================================//
type MultiCh [4]chan HdpEvent

// ==================================================================
func (c MultiCh) SendEvent(i int, data ...any) HdpEvent {
	return c.SendEvent1(i, NewHdpEvent(data...))
}

// ==================================================================
func (c MultiCh) SendEvent1(i int, req HdpEvent) HdpEvent {
	// logger.Debugf("got event : %v", req)
	readyCh := make(chan bool, 1)
	go func() {
		<-readyCh
		c[i] <- req
	}()
	req["readyCh"] = readyCh
	return req
}
