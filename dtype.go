package aionet

import (
	"fmt"
	"math"
	"time"

	"github.com/pcbuildpluscoding/aionet/dtype"
	tpt "github.com/pcbuildpluscoding/transport"
)

// ==================================================================//
// fdOpen
// ==================================================================
type fdOpen struct {
	fd    int
	err   error
	pname string
}

// ==================================================================
func (x fdOpen) tell() (int, error, string) {
	return x.fd, x.err, x.pname
}

// ==================================================================//
// Listener
// ==================================================================//
type Listener interface {
	Accept() (*HdpConn, error)
}

// ==================================================================//
// Dialer
// ==================================================================//
type Dialer interface {
	Dial(dtype.HdpEvent) (*HdpConn, error)
}

// ==================================================================//
// Statet
// ==================================================================//
type Statet func(dtype.HdpEvent) (Statet, dtype.HdpEvent)

// ================================================================//
// Timer
// ================================================================//
type funcTimeout func(uint32) func()
type Timer map[uint32]timer

// ---------------------------------------------------------------//
// New
// ---------------------------------------------------------------//
func (tr Timer) New(timeout uint16, req dtype.HdpEvent, f funcTimeout) uint32 {
	key := uint32(time.Now().UnixMicro() % math.MaxUint32)
	t := timer{}
	if timeout > 0 {
		dura := time.Duration(timeout) * time.Millisecond
		t.t = time.AfterFunc(dura, f(key))
	}
	t.f = func() dtype.HdpEvent { return req }
	tr[key] = t
	return key
}

// ---------------------------------------------------------------//
// stopTimer
// ---------------------------------------------------------------//
func (tr Timer) Stop(key uint32) (res dtype.HdpEvent) {
	// key is a timestamp
	if _, found := tr[key]; !found {
		return res.With1(fmt.Errorf("Timer[%s] does not exist", time.UnixMilli(int64(key)).Format(tpt.MilliDatestamp)))
	}
	t := tr[key]
	if t.t != nil {
		t.t.Stop()
	}
	if t.f == nil {
		panic(fmt.Errorf("Timer error : resume func is nil"))
	}
	delete(tr, key)
	return t.f()
}

// ================================================================//
// Timer
// ================================================================//
type timer struct {
	t *time.Timer
	f func() dtype.HdpEvent
}

// ---------------------------------------------------------------//
// New
// ---------------------------------------------------------------//
func (t *timer) new(timeout uint16, req dtype.HdpEvent, f func()) {
	if timeout > 0 {
		dura := time.Duration(timeout) * time.Millisecond
		t.t = time.AfterFunc(dura, f)
	}
	t.f = func() dtype.HdpEvent { return req }
}

// ---------------------------------------------------------------//
// stopTimer
// ---------------------------------------------------------------//
func (t *timer) stop() dtype.HdpEvent {
	if t.t != nil {
		t.t.Stop()
	}
	if t.f == nil {
		panic(fmt.Errorf("Timer error : resume func is nil"))
	}
	return t.f()
}

// ==================================================================//
// EpollEvent
// ==================================================================//
type EpollEvent struct {
	cid   string
	fd    uint32
	flags ioMode
}

// ==================================================================//
// ioEvent
// ==================================================================//
type ioEvent struct {
	ch       chan error
	dlRef    uint16
	epoll    EpollEvent
	err      error
	pipeName string
	timedAt  time.Time
}

// ==================================================================//
// interrupt
// ==================================================================//
type interrupt struct {
	aux  any
	code int
	err  error
}

func (i *interrupt) Error() string {
	return i.err.Error()
}

// ==================================================================//
// reqRef
// ==================================================================//
type reqRef struct {
	cid      string
	fd       uint32
	flags    ioMode
	deadline time.Time
	pipeName string
}

// ==================================================================//
// ioReq
// ==================================================================//
type ioReq struct {
	aux []any
	ch  chan error
	op  int
	ref reqRef
}

// ==================================================================
func (r *ioReq) sendError(flags ioMode, err error) func() {
	ch := r.ch
	r.ch = nil
	return func() {
		if ch == nil {
			return
		}
		dura := time.Duration(100) * time.Microsecond
		select {
		case <-time.After(dura):
			logger.Errorf("%s |%s| result was not consumed before deadline, error : %v ...", r.ref.cid, flags.String(), err)
		case ch <- err:
		}
	}
}

type Void struct{}

// ==================================================================//
// pipeAddr
// ==================================================================//
type pipeAddr struct {
	emtype string // emulated network type
	fd     int
	name   string
}

func (p pipeAddr) Network() string {
	return p.emtype
}

func (p pipeAddr) String() string {
	return fmt.Sprintf("%d:%s", p.fd, p.name)
}

// ==================================================================
type UniconnReq struct {
	cid, pollsubject, serverAddr string
	mode                         int
	resultCh                     chan dtype.HdpEvent
}
