package epoller

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// ==================================================================//
// fdError
// ==================================================================//
type fdError struct {
	Fd [2]int
}

func (e fdError) Error() string {
	return fmt.Sprintf("mocket fd endpoints : %v", e.Fd)
}

// ==================================================================//
// reqRef
// ==================================================================//
type reqRef struct {
	cid      string
	fd       int
	flags    int
	mode     ioMode
	deadline time.Time
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
// ioReq
// ==================================================================//
type ioReq struct {
	aux any
	ch  chan error
	op  int
	ref reqRef
}

// ==================================================================
func (r *ioReq) sendError(mode ioMode, err error) func() {
	ch := r.ch
	r.ch = nil
	return func() {
		if ch == nil {
			return
		}
		dura := time.Duration(100) * time.Microsecond
		select {
		case <-time.After(dura):
			logger.Errorf("%s |%s| err result was not consumed before deadline, error : %v ...", r.ref.cid, mode.String(), err)
		case ch <- err:
		}
	}
}

// ==================================================================//
// ioEvent
// ==================================================================//
type ioEvent struct {
	dlRef   uint16
	pev     []unix.EpollEvent
	err     error
	req     ioReq
	timedAt time.Time
}

// ==================================================================//
// ioRace
// ==================================================================//
type ioRace struct {
	armed   bool
	cid     string
	ch      chan error
	dlRef   uint16
	err     error
	doneCh  map[uint16]chan error
	mode    ioMode
	ready   bool
	timedAt time.Time
}

// ==================================================================
func (r *ioRace) cancelDeadline(dlRef uint16) {
	ch := r.doneCh[dlRef]
	if ch != nil {
		close(ch)
		delete(r.doneCh, dlRef)
	}
}

// ==================================================================
func (r *ioRace) cancelDeadlines(mode ioMode, err error) {
	r.err = err
	if r.doneCh == nil {
		logger.Errorf("%s can't cancel %s deadlines, doneCh is nil", r.cid, mode)
		return
	}
	for dlRef := range r.doneCh {
		// logger.Debugf("%s %d io operation is cancelled ...", r.cid, flags)
		r.cancelDeadline(dlRef)
	}
	if r.ch != nil {
		go r.deleteResultCh(mode, err)()
	} else {
		logger.Debugf("%s does not have an ioRace response channel !!", r.cid)
	}
}

// ==================================================================
func (r *ioRace) cancelIoByKey(mode ioMode, dlRef uint16, err error) {
	if r.doneCh == nil {
		logger.Errorf("%s can't cancel %s deadline, doneCh is nil", r.cid, mode.String())
		return
	}
	r.cancelDeadline(dlRef)
	if r.ch != nil {
		logger.Debugf("@@@@@@@@@ sending %s error to %s, error : %v", mode.String(), r.cid, err)
		go r.deleteResultCh(mode, err)()
	} else {
		logger.Warnf("$$$$$$$$$ %s %s race result channel is nil !!!!!!", r.cid, mode.String())
	}
}

// ==================================================================
func (r *ioRace) deleteResultCh(mode ioMode, err error) func() {
	ch := r.ch
	r.ch = nil
	return func() {
		dura := time.Duration(1) * time.Second
		select {
		case <-time.After(dura):
			logger.Errorf("%s %s err result was not consumed before 1 second deadline ...", r.cid, mode.String())
		case ch <- err:
		}
	}
}

// ==================================================================
func (r *ioRace) doIoReady(mode ioMode) {
	r.err = nil
	r.ready = false
	r.cancelDeadlines(mode, nil)
	if r.ch != nil {
		go r.deleteResultCh(mode, nil)()
	}
}

// ==================================================================
func (r *ioRace) doIoTimeout(mode ioMode, ev ioEvent) {
	if r.timedAt != zeroTime && ev.timedAt.After(r.timedAt) {
		logger.Debugf("%s ignoring stale %s timeout event ...", r.cid, mode.String())
		return
	}
	r.err = ev.err
	if r.doneCh == nil {
		logger.Errorf("%s can't cancel %s deadline, doneCh is nil", r.cid, mode.String())
		return
	}
	r.cancelDeadline(ev.dlRef)
	if r.ch != nil {
		logger.Debugf("@@@@@@@@@ sending %s error to %s, error : %v", mode.String(), r.cid, ev.err)
		go r.deleteResultCh(mode, ev.err)()
	} else {
		logger.Warnf("$$$$$$$$$ %s %s race result channel is nil !!!!!!", r.cid, mode.String())
	}
}

// ==================================================================
func (r *ioRace) handle(mode ioMode, dlRef uint16, err error) {
	r.err = err
	if err == nil {
		// logger.Debugf("%s io-ready event won the race so flushing all deadlines ...", r.cid)
		r.cancelDeadlines(mode, nil)
	} else {
		logger.Debugf("%s deadline-expiration[%d] won the race, reporting timeout error ...", r.cid, dlRef)
		r.cancelIoByKey(mode, dlRef, err)
	}
}

// ---------------------------------------------------------------//
// newDoneCh
// ---------------------------------------------------------------//
func (r *ioRace) newDoneCh() (uint16, chan error) {
	doneCh := make(chan error, 1)
	r.dlRef++
	dlRef := r.dlRef
	r.doneCh[dlRef] = doneCh
	return dlRef, doneCh
}

// ==================================================================//
// epoller
// ==================================================================//
type epoller struct {
	cpuid int
	efd   int // eventfd
	pfd   int // epoll fd

	eventCh chan ioEvent
	race    map[int][2]*ioRace
	running bool
}

// ==================================================================
func (p *epoller) cancelIo(req ioReq) error {
	ref := req.ref
	logger.Debugf("############# cancelIO is cancelling %s %s request", ref.cid, ref.mode.String())
	race := p.getIoRace(ref)
	if race == nil {
		return fmt.Errorf("%s %s ioRace is undefined", ref.cid, ref.mode.String())
	}
	if race.err != nil {
		logger.Debugf("%s has a persisting ioRace error : %v", ref.cid, race.err)
		return race.err
	}
	logger.Debugf("@@@@@@@@@@@@ cancelIO is cancelling %s %s request", race.cid, ref.mode.String())
	race.cancelDeadlines(ref.mode, nil)
	return nil
}

// ==================================================================
func (p *epoller) checkAffinity() bool {
	if p.cpuid == 0 {
		// wakeup called for shutdown
		return false
	}
	logger.Debugf("setting loop affinity ...")
	err := setAffinity(p.cpuid)
	if err != nil {
		logger.Errorf("poller setAffinity error : %v", err)
		return false
	}
	p.cpuid = -1
	return true
}

// ==================================================================
func (p *epoller) Close() error {
	return p.wakeup(0)
}

// ==================================================================
func (p *epoller) deleteByFd(ref reqRef) error {
	err := NewHdpError(ErrClosed, ref.cid)
	race := p.getIoRace(ref)
	if race != nil {
		race.cancelDeadlines(ref.mode, err)
	}
	delete(p.race, ref.fd)
	return unix.Close(ref.fd)
}

// ==================================================================
func (p *epoller) getEventCount() error {
	ebuf := make([]byte, 8)
	_, err := unix.Read(p.efd, ebuf)
	return err
}

// ==================================================================
func (p *epoller) getIoRace(ref reqRef) *ioRace {
	race := p.race[ref.fd]
	i := R
	if ref.mode == EV_WRITE {
		i = W
	}
	if race[i] == nil {
		return nil
	}
	return race[i]
}

// ==================================================================
func (p *epoller) handleIoError(ev ioEvent) {
	if errors.Is(ev.err, os.ErrDeadlineExceeded) {
		for _, pev := range ev.pev {
			p.handleTimeout(pev, ev)
		}
	} else if len(ev.pev) > 0 {
		ref := ev.req.ref
		race := p.getIoRace(ref)
		if race != nil {
			logger.Debugf("%s got non-timeout io error : %v", race.cid, ev.err)
		}
	}
}

// ==================================================================
func (p *epoller) handleIoReady(pev unix.EpollEvent, ev ioEvent) {
	// all events are handled as a oneshot event, so remove it from storage
	key := int(pev.Fd)
	race := p.race[key]
	logger.Debugf("@@@@@@@@@@@@@@ got io-ready events : %d for %s,%s", pev.Events, race[R].cid, race[W].cid)
	rearmed := false
	switch {
	case race == [2]*ioRace{}:
		logger.Errorf("fd[%d] EV_READ ioRace does not exist for events, error : %x, %v", pev.Fd, pev.Events, ev.err)
	case pev.Events&PEV_ERROR != 0:
		logger.Debugf("%s got EV_ERROR ...", race[R].cid)
		return
	case pev.Events&unix.EPOLLIN != 0:
		if race[R].ch != nil {
			logger.Debugf("%s is returning read-readiness ...", race[R].cid)
			race[R].timedAt = ev.timedAt
			race[R].doIoReady(EV_READ)
		} else {
			logger.Debugf("%s is read-ready ...", race[R].cid)
			race[R].ready = true
		}
		race[R].armed = false
	default:
		// bug-fix : confirm that a read-ready request exists for this fd
		// rearm registered read event, otherwise epoller effectively deletes the previous read-ready state
		if race[R].ch != nil {
			err := p.rearm1(reqRef{
				fd:   int(pev.Fd),
				mode: race[R].mode,
			})
			race[R].armed = true
			rearmed = true
			logger.Debugf("%s %s readiness polling is rearmed : %v ...", race[R].cid, race[R].mode.String(), err)
		}
	}
	switch {
	case pev.Events&unix.EPOLLOUT != 0:
		if race[W].ch != nil {
			logger.Debugf("%s is returning write-readiness ...", race[W].cid)
			race[W].timedAt = ev.timedAt
			race[W].doIoReady(EV_WRITE)
		} else {
			logger.Debugf("%s is write-ready ...", race[W].cid)
			race[W].ready = true
		}
		race[W].armed = false
	default:
		// bug-fix : confirm that a read-ready request exists for this fd
		// rearm registered read event, otherwise epoller effectively deletes the previous read-ready state
		if race[W].ch != nil && !rearmed {
			err := p.rearm1(reqRef{
				fd:   int(pev.Fd),
				mode: race[W].mode,
			})
			race[W].armed = true
			logger.Debugf("%s %s readiness polling is rearmed : %v ...", race[W].cid, race[W].mode.String(), err)
		}
	}
}

// ==================================================================
func (p *epoller) handleReq(req ioReq) {
	// token := p.lock.Get("epoller", "poller.go/epoller.handleEvent", 352)

	var err error
	ref := req.ref
	logger.Debugf("############ poller got cid, fd, op, flags, deadline : %s, %d, %d, %x, %s", ref.cid, ref.fd, req.op, ref.flags, ref.deadline.Format("02-15-04-05.000000"))
	switch {
	case req.op == unix.EPOLL_CTL_DEL:
		// don't need to call Epollctl(p.pfd, unix.EPOLL_CTL_DEL ...) because according to the epoll7 document
		// the fd will be removed from the epoll interest list. CAVEAT : A file descriptor is removed from an interest
		// list only after all the file descriptors referring to the underlying open file description have been closed
		logger.Debugf("########## calling epoller.deleteByFd1(%d) for conn %s ##########", ref.fd, ref.cid)
		err = p.deleteByFd(ref)
		logger.Debugf("########## got unix.Close(%d) error : %v", ref.fd, err)
	case req.op == unix.EPOLL_CTL_ADD:
		err = p.watch(ref)
		// returnErr = true
	case req.op == unix.EPOLL_CTL_MOD:
		if ref.flags&PEV_RESET != 0 {
			p.resetIoReady(req.ref)
			break
		}
		err = p.rearm(ref, req.ch)
		if err == nil {
			return
		}
		// logger.Debugf("epoller rearm err result : %v, flags : %d", err, ref.flags)
	case req.op == unix.EPOLLHUP:
		logger.Debugf("%s io activity is cancelled !!", ref.cid)
		err = p.cancelIo(req)
	case req.op == 0:
		if ref.deadline.Equal(zeroTime) {
			err = p.cancelIo(req)
		} else {
			err = p.raceDeadline(ref)
		}
	default:
		err = p.others(req)
	}
	go req.sendError(ref.mode, err)()
}

// ==================================================================
func (p *epoller) handleTimeout(pev unix.EpollEvent, ev ioEvent) {
	// token := p.lock.Get("epoller", "epoller.handleTimeout", 0)

	logger.Debugf("epoller is handling an io-timeout event ...")
	key := int(pev.Fd)
	ref := ev.req.ref
	race := p.getIoRace(ref)
	switch {
	case race == nil:
		logger.Errorf("fd[%d] ioRace does not exist. error : %v", key, ev.err)
	case ref.mode == EV_READ:
		if pev.Events&PEV_READ != 0 {
			race.doIoTimeout(EV_READ, ev)
		}
	case ref.mode == EV_WRITE:
		if pev.Events&PEV_WRITE != 0 {
			race.doIoTimeout(EV_WRITE, ev)
		}
	}
}

// ==================================================================
func (p *epoller) others(req ioReq) error {
	if req.ref.fd == p.efd {
		return p.putEventCount(1)
	}
	return nil
}

// ==================================================================
func (p *epoller) putEventCount(ecount int) error {
	ebuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(ebuf, uint64(ecount))
	_, err := unix.Write(p.efd, ebuf)
	return err
}

// ==================================================================
func (p *epoller) raceDeadline(ref reqRef) error {
	logger.Debugf("%s is adding a new deadline racer ...", ref.cid)
	race := p.getIoRace(ref)
	if race == nil {
		return fmt.Errorf("%s ioRace is undefined", ref.cid)
	}
	// event, doneCh := p.newRaceEvent(key, req.ref)
	if errors.Is(race.err, VErrTimeout) {
		logger.Debugf("%s newRaceEvent has cleared a persisting timeout error ...", ref.cid)
		race.err = nil
	}
	race.timedAt = zeroTime
	dlRef, doneCh := race.newDoneCh()
	logger.Debugf("%s got new ioRace and doneCh, ref.flags, deadline ref : %d, %d", ref.cid, ref.flags, dlRef)
	event := newIoEvent(ref, dlRef, unix.EpollEvent{
		Events: uint32(ref.flags),
		Fd:     int32(ref.fd),
	})
	go p.raceDeadline1(ref, event, doneCh)()
	return race.err
}

// ==================================================================
func (p *epoller) raceDeadline1(ref reqRef, event ioEvent, doneCh chan error) func() {
	return func() {
		// waiting until deadline
		// logger.Debugf("epoller is adding a deadline timer for cid : %s", ref.cid)
		dura := time.Until(ref.deadline)
		timer := time.NewTimer(dura)
		defer timer.Stop()
		select {
		case <-doneCh:
			// logger.Debugf("%s epoller won the %x race beat the deadline !!", ref.cid, ref.flags)
			return
		case t := <-timer.C:
			event.timedAt = t
			logger.Debugf("%s epoller %s event deadline expired ...", ref.cid, PevString(event.pev[0].Events))
		}
		p.eventCh <- event
	}
}

// ==================================================================
func (p *epoller) rearm(ref reqRef, ch chan error) error {
	race := p.getIoRace(ref)
	if race == nil {
		return fmt.Errorf("%s ioRace is undefined", ref.cid)
	}
	logger.Debugf("%s wants to rearm %s-readiness, ready-now, error : %v, %v", race.cid, ref.mode.String(), race.ready, race.err)
	if race.err != nil {
		race.ready = false
		ch <- race.err
		return nil
	} else if race.ready {
		race.ready = false
		ch <- nil
		return nil
	}
	race.ch = ch
	// next syscall is protected by the wait select loop
	flags := unix.EPOLLONESHOT | unix.EPOLLET
	flags |= ref.flags
	err := unix.EpollCtl(p.pfd, unix.EPOLL_CTL_MOD, ref.fd,
		&unix.EpollEvent{Fd: int32(ref.fd), Events: uint32(flags)})
	race.armed = true
	logger.Debugf("%s %s readiness polling is armed : %v ...", race.cid, ref.mode.String(), err)
	return err
}

// ==================================================================
func (p *epoller) rearm1(ref reqRef) error {
	flags := unix.EPOLLONESHOT | unix.EPOLLET
	switch ref.mode {
	case EV_READ:
		flags |= unix.EPOLLIN
	case EV_WRITE:
		flags |= unix.EPOLLOUT
	}
	return unix.EpollCtl(p.pfd, unix.EPOLL_CTL_MOD, ref.fd,
		&unix.EpollEvent{Fd: int32(ref.fd), Events: uint32(flags)})
}

// ==================================================================
func (p *epoller) resetIoReady(ref reqRef) {
	// all events are handled as a oneshot event, so remove it from storage
	race := p.getIoRace(ref)
	if race != nil {
		race.ready = false
		race.armed = false
		logger.Debugf("%s %s-ready status and polling armed status is reset", race.cid, ref.mode.String())
	}
}

// ==================================================================
func (p *epoller) run() {
	// close poller fd & eventfd in defer
	defer p.shutdown()

	p.race = map[int][2]*ioRace{}
	p.eventCh = make(chan ioEvent, 1)
	p.running = true

	go p.epollWait()

	for p.running {
		x := <-p.eventCh
		switch {
		case x.err != nil:
			p.handleIoError(x)
			// logger.Errorf("epollWait error : %v", x.err)
		case len(x.pev) == 0:
			p.handleReq(x.req)
		default:
			for _, pev := range x.pev {
				fd := int(pev.Fd)
				if fd == p.efd {
					p.running = p.checkAffinity()
				}
				// logger.Debugf("event flags[%d] : %x\n", int(ev.Fd), ev.Events)
				p.handleIoReady(pev, x)
			}
		}
	}
	logger.Debugf("epoller event handler is now complete.")
}

// ==================================================================
func (p *epoller) SetAffinity(cpuid int) error {
	// token := p.lock.Get("epoller", "poller.go/epoller.SetAffinity", 537)
	if cpuid >= runtime.NumCPU() {
		return fmt.Errorf("core %d does not exist", cpuid)
	}
	// store and wakeup
	return p.wakeup(cpuid)
}

// ==================================================================
func (p *epoller) shutdown() {
	// token := p.lock.Get("epoller", "poller.go/epoller.shutdown", 560)
	logger.Debugf("poller is stopping ...")
	unix.Close(p.pfd)
	p.pfd = -1
	p.efd = -1
	logger.Debugf("poller is now stopped.")
}

// ==================================================================
func (p *epoller) epollWait() {
	x := ioEvent{}
	ebuf := newBuffer()

	for p.running {
		n, err := unix.EpollWait(p.pfd, ebuf.this, -1)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			logger.Errorf("got epollWait error : %v", err)
			p.eventCh <- ioEvent{err: err}
			return
		}

		x.pev = make([]unix.EpollEvent, n)
		for i := range n {
			ev := ebuf.this[i]
			if int(ev.Fd) == p.efd {
				// wakeup event for shutdown - wakeup is required if idle
				_ = p.getEventCount()
				if p.cpuid == 0 {
					p.running = false
				}
			}
			x.pev[i] = ev
			ebuf.this[i].Events = 0
		}
		p.eventCh <- x
		ebuf.resize(n)
	}
	logger.Debugf("poller event listener is now complete.")
}

// ==================================================================
func (p *epoller) wakeup(cpuid int) error {
	p.cpuid = cpuid
	ch := make(chan error, 1)
	req := ioReq{
		ch: ch,
		ref: reqRef{
			fd:    p.efd,
			flags: unix.EPOLLIN, // bugfix on 17/5/24: added correct flag
		},
	}
	p.eventCh <- ioEvent{req: req}
	return <-ch
}

// ==================================================================
func (p *epoller) watch(ref reqRef) error {
	cid := strings.Split(ref.cid, "|")
	p.race[ref.fd] = [2]*ioRace{newIoRace(cid[R], EV_READ), newIoRace(cid[W], EV_WRITE)}

	logger.Debugf("epoller is calling a watch on conn[%d] with cid[%s, %s]", ref.fd, cid[R], cid[W])
	return unix.EpollCtl(p.pfd, unix.EPOLL_CTL_ADD, ref.fd,
		&unix.EpollEvent{Fd: int32(ref.fd), Events: uint32(ref.flags)})
}

// ==================================================================//
// eventBuffer
// ==================================================================//
type eventBuffer struct {
	this            []unix.EpollEvent
	sizeLog         [2]int // buffer size history
	sinceLastResize int
}

// ==================================================================
func (b *eventBuffer) resize(ecount int) {
	size := len(b.this)
	delta := 16
	if b.sinceLastResize > 2 {
		if b.upSizable(ecount) {
			b.sinceLastResize = 0
			if (size + delta) > maxEvents {
				delta = maxEvents - size
			}
			x := make([]unix.EpollEvent, delta)
			b.this = append(b.this, x...)
		} else if b.downSizable(ecount) {
			b.sinceLastResize = 0
			newSize := size - delta
			if newSize < minEvents {
				newSize = minEvents
			}
			b.this = b.this[:newSize]
		}
	}
	b.sizeLog[0] = b.sizeLog[1]
	b.sizeLog[1] = ecount
	b.sinceLastResize++
}

// ==================================================================
func (b eventBuffer) downSizable(ecount int) bool {
	// if the event count was <= buffSize-16 for the last 3 iterations, return true
	if ecount > len(b.this)-16 {
		return false
	}
	for _, prevCount := range b.sizeLog {
		if prevCount > len(b.this)-16 {
			return false
		}
	}
	return true
}

// ==================================================================
func (b eventBuffer) upSizable(ecount int) bool {
	// if the buffer was full for the last 3 iterations, return true
	if ecount < len(b.this) {
		return false
	}
	for _, prevCount := range b.sizeLog {
		if prevCount < len(b.this) {
			return false
		}
	}
	return true
}
