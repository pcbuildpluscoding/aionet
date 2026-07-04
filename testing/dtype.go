package test

import (
	"fmt"
	"testing"

	aio "github.com/pcbuildpluscoding/aionet"
)

// ==================================================================//
// Result
// ==================================================================//
type Result map[string]any

// ====================================================================
func (r Result) add(args ...any) error {
	var (
		key string
		ok  bool
	)
	for i, arg := range args {
		switch i % 2 {
		case 0:
			if key, ok = arg.(string); !ok {
				return fmt.Errorf("key string, value any sequence format is required.")
			}
		default:
			r[key] = arg
		}
	}
	return nil
}

// ====================================================================
func (r Result) Add(args ...any) error {
	for i, arg := range args {
		switch val := arg.(type) {
		case nil:
			r["error"] = nil
		case error:
			r["error"] = val
		case chan Result:
			r["resultCh"] = val
		case string:
			if val == ":data" {
				if j := i + 1; j < len(args) {
					return r.add(args[j:]...)
				}
			}
		default:
		}
	}
	return nil
}

// ====================================================================
func (r Result) Err(arg ...string) error {
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

// ====================================================================
func (r Result) HdpConn(key string) *aio.HdpConn {
	switch c := r[key].(type) {
	case *aio.HdpConn:
		return c
	default:
		return nil
	}
}

// ====================================================================
func (r Result) SeqNum(key string) uint32 {
	switch n := r[key].(type) {
	case uint32:
		return n
	default:
		return 0
	}
}

// ====================================================================
func (r Result) String(key string) string {
	switch s := r[key].(type) {
	case []byte:
		return string(s)
	case string:
		return s
	}
	return ""
}

// ====================================================================
func (r Result) Value(key string) any {
	return r[key]
}

// ====================================================================
func (r Result) With(args ...any) Result {
	if r == nil {
		r = Result{}
	}
	err := r.Add(args...)
	if err != nil {
		r["error"] = err
	}
	return r
}

type testFunc func(*testing.T, Result, ...any) Result

// ==================================================================//
// Testcase
// ==================================================================//
type Testcase = Result

// ====================================================================
func (tc Testcase) Tcfunc(key string) testFunc {
	switch tf := tc[key].(type) {
	case testFunc:
		logger.Debugf("Testcase is returning a testFunc !!")
		return tf
	}
	return nil
}
