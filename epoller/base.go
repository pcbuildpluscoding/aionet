package epoller

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/logroll"
	"golang.org/x/sys/unix"
)

var (
	logger   logroll.Logger
	poller   epoller // poller
	zeroTime = time.Time{}
)

type Void struct{}

// ==================================================================
func init() {
	var err error
	logger = logroll.New()
	poller, err = newEPoller()
	if err != nil {
		panic(err)
	}
	go poller.run()
}

// ==================================================================
func SetLogger(super *logroll.LogFile) {
	logger = super
}

// ==================================================================
func Stop() {
	poller.running = false
	poller.eventCh <- ioEvent{}
}

// ==================================================================
func newBuffer() eventBuffer {
	return eventBuffer{
		sizeLog: [2]int{0, 0},
		this:    make([]unix.EpollEvent, minEvents),
	}
}

// ---------------------------------------------------------------//
// newEpoller
// ---------------------------------------------------------------//
func newEPoller() (epoller, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return epoller{}, fmt.Errorf("poller creation error : %v", err)
	}
	r0, _, e0 := unix.Syscall(unix.SYS_EVENTFD2, 0, unix.EFD_NONBLOCK, 0)
	if e0 != 0 {
		unix.Close(fd)
		return epoller{}, fmt.Errorf("poller set non-blocking eventFd unix error : %v", err)
	}

	err = unix.EpollCtl(fd, unix.EPOLL_CTL_ADD, int(r0),
		&unix.EpollEvent{Fd: int32(r0),
			Events: unix.EPOLLIN | unix.EPOLLET,
		},
	)
	if err != nil {
		unix.Close(fd)
		unix.Close(int(r0))
		return epoller{}, fmt.Errorf("adding eventFd for reading failed : %v", err)
	}

	p := new(epoller)
	p.cpuid = -1
	p.efd = int(r0)
	p.pfd = fd
	return *p, err
}

// ==================================================================
func newIoEvent(ref reqRef, dlRef uint16, pev ...unix.EpollEvent) ioEvent {
	var err error
	switch {
	case ref.deadline != zeroTime:
		err = os.ErrDeadlineExceeded //err = dtype.NewHdpError(ErrTimeout, ref.cid)
	default:
		err = dtype.NewHdpError(ErrCancelled, ref.cid) // cancelled
	}
	req := ioReq{
		ref: ref,
	}
	return ioEvent{
		dlRef: dlRef,
		err:   err,
		pev:   pev,
		req:   req,
	}
}

// ===========================================================================
func newIoRace(ref reqRef) *ioRace {
	return &ioRace{
		cid:    ref.cid,
		mode:   ref.mode,
		doneCh: map[uint16]chan error{},
	}
}

// ===========================================================================
// bind thread & goroutine to a specific CPU
func setAffinity(cpuId int) error {
	runtime.LockOSThread()

	var cpuset unix.CPUSet
	cpuset.Set(int(cpuId))

	return unix.SchedSetaffinity(0, &cpuset)
}

// ===========================================================================
func SubmitIoReq(ev dtype.HdpEvent) chan error {
	logger.Debugf("@@@@@@@@ got new io-ready request : %v", ev)
	ch := make(chan error, 1)
	req := ioReq{
		op: ev.Int("ioReq/op"),
		ch: ch,
	}
	ref := reqRef{}
	for key, ival := range ev {
		switch key {
		case "ioReq/aux":
			req.aux, _ = ival.([]any)
		case "reqRef/cid":
			ref.cid, _ = ival.(string)
		case "reqRef/fd":
			ref.fd, _ = ival.(int)
		case "reqRef/flags":
			ref.flags, _ = ival.(int)
		case "reqRef/mode":
			x, _ := ival.(int)
			ref.mode = ioMode(x)
		case "reqRef/deadline":
			ref.deadline, _ = ival.(time.Time)
			logger.Debugf("$$$$$$$$$ got deadline timestamp : %s", ref.deadline.String())
		}
	}
	req.ref = ref
	poller.eventCh <- ioEvent{req: req}
	return ch
}

// ===========================================================================
func PevString(pev uint32) string {
	switch pev {
	case PEV_ERROR:
		return "PEV_ERROR"
	case PEV_READ:
		return "PEV_READ"
	case PEV_RESET:
		return "PEV_RESET"
	case PEV_WRITE:
		return "PEV_WRITE"
	}
	return "INVALID"
}
