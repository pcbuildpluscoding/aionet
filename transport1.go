package aionet

import (
	"io"
	"syscall"
	"time"

	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/aionet/epoller"
	"golang.org/x/sys/unix"
)

// ===========================================================================
type hdpRead2 struct {
	*socket
	refNum *[2]uint16
	tpt    dtype.MultiCh
}

// ===========================================================================
func (c *hdpRead2) handle(req dtype.HdpEvent) {
	logger.Debugf("%s is handling a request : %v ...", c.cid, req)
	switch req.Flag1() {
	case dtype.HDP_INIT1:
		logger.Debugf("%s is starting with fd : %d", c.cid, c.fd)
	default:
	}
}

// ===========================================================================
func (c *hdpRead2) listen(b []byte) (int, error) {
	// don't need to lock here because AioListener is the exclusive reader of <l.connex.pname>.accept
	logger.Debugf("############## %s is listening : ##################", c.cid)
	err := <-waitIoReady(c.cid, c.fd, EV_READ)
	logger.Debugf("@@@@@@@@@@@@@@@@@ %s got io-ready result : %v", c.cid, err)
	if err != nil {
		return 0, err
	}
	for range 3 {
		n, err := c.read(b)
		if err != nil {
			switch err {
			case unix.EAGAIN:
				<-time.After(100 * time.Millisecond)
				continue
			}
			return 0, err
		}
		return n, nil
	}
	logger.Debugf("@@@@@@@@@@@@@@ %s exhausted EAGAIN read retries ...", c.cid)
	return 0, io.EOF
}

// ===========================================================================
func (c *hdpRead2) run() {
	logger.Debugf("%s[%d] is running ...", c.cid, c.fd)
	for ev := range c.tpt[R] {
		err := ev.Err()
		if err != nil {
			logger.Debugf("@@@@@@@@@@@ %s got an error : %v @@@@@@@@@@@@@@@@", c.cid, ev.Err())
			return
		}
		logger.Debugf("@@@@@@@@@@@ %s got a new conn event : %v @@@@@@@@@@@@@@@@", c.cid, ev)
		switch ev.Flag1() {
		case dtype.HDP_DATAGRAM1:
			if ev.Value("deadline") == nil {
				c.readFrame(ev)
				continue
			}
			c.setDeadline(ev)
		case dtype.HDP_RESET1:
			logger.Debugf("@@@@@@@@@@@@@@@ %s got a HDP_RESET1 event @@@@@@@@@@@@@@@@", c.cid)
			return
		default:
			c.handle(ev)
		}
	}
}

// ===========================================================================
func (c *hdpRead2) readFrame(req dtype.HdpEvent) {
	logger.Debugf("$$$$$$$$$$$ %s[%d] is reading a new frame ...", c.cid, c.fd)
	b := req.Bytes()
	n, err := c.listen(b)
	if err != nil {
		logger.Debugf("%s got reading error : %v", c.cid, err)
		req.Ch() <- newHdpEvent(err)
		return
	}
	logger.Debugf("%s returning HDP_DATAGRAM read result : %s", c.cid, b)
	req.Ch() <- req.With(err, ":data", "byteNum", n)
}

// ===========================================================================
func (c *hdpRead2) setDeadline(req dtype.HdpEvent) {
	logger.Debugf("$$$$$$$$$$$ %s[%d] is setting a read deadline ...", c.cid, c.fd)
	err := <-epoller.SubmitIoReq(newHdpEvent(":data",
		"reqRef/cid", c.cid,
		"reqRef/fd", c.fd,
		"reqRef/flags", int(epoller.PEV_READ),
		"reqRef/deadline", req.Value("deadline")))
	req.Ch() <- req.With(err)
}

// ===========================================================================
func (c *hdpRead2) Start() dtype.HdpEvent {
	ev := newHdpEvent(":data",
		"ioReq/op", unix.EPOLL_CTL_ADD,
		"reqRef/cid", c.cid,
		"reqRef/fd", c.fd,
		"reqRef/flags", syscall.EPOLLONESHOT|syscall.EPOLLIN|unix.EPOLLET,
		"reqRef/mode", int(EV_READ),
	)
	err := <-epoller.SubmitIoReq(ev)
	if err != nil {
		return ev.Withf(500, "epoller watch request failed : %v", err)
	}
	go c.run()
	return ev
}

// ===========================================================================
type hdpWrite2 struct {
	*socket
	refNum *[2]uint16
	tpt    dtype.MultiCh
}

// ===========================================================================
func (c *hdpWrite2) handle(req dtype.HdpEvent) {
	logger.Debugf("%s is handling a request : %v ...", c.cid, req)
	switch req.Flag1() {
	case dtype.HDP_DATAGRAM1:
		// write the data and send back the write result to the user
	default:
	}
}

// ===========================================================================
func (c *hdpWrite2) run() {
	logger.Debugf("%s is running ...", c.cid)
	for ev := range c.tpt[W] {
		err := ev.Err()
		if err != nil {
			logger.Debugf("@@@@@@@@@@@ %s got an error : %v @@@@@@@@@@@@@@@@", c.cid, ev.Err())
			return
		}
		logger.Debugf("@@@@@@@@@@@ %s got a new conn event : %v @@@@@@@@@@@@@@@@", c.cid, ev)
		switch ev.Flag1() {
		case dtype.HDP_DATAGRAM1:
			if ev.Value("deadline") == nil {
				c.writeFrame(ev)
				continue
			}
			c.setDeadline(ev)
		case dtype.HDP_RESET1:
			logger.Debugf("@@@@@@@@@@@@@@@ %s got a HDP_RESET1 event @@@@@@@@@@@@@@@@", c.cid)
			return
		default:
			c.handle(ev)
		}
	}
}

// ===========================================================================
func (c *hdpWrite2) setDeadline(req dtype.HdpEvent) {
	logger.Debugf("$$$$$$$$$$$ %s[%d] is setting a read deadline ...", c.cid, c.fd)
	err := <-epoller.SubmitIoReq(newHdpEvent(":data",
		"reqRef/cid", c.cid,
		"reqRef/fd", c.fd,
		"reqRef/flags", int(epoller.PEV_WRITE),
		"reqRef/deadline", req.Value("deadline")))
	req.Ch() <- req.With(err)
}

// ===========================================================================
func (c *hdpWrite2) Start() dtype.HdpEvent {
	ev := newHdpEvent(":data",
		"ioReq/op", unix.EPOLL_CTL_ADD,
		"reqRef/cid", c.cid,
		"reqRef/fd", c.fd,
		"reqRef/flags", syscall.EPOLLONESHOT|syscall.EPOLLOUT|unix.EPOLLET,
		"reqRef/mode", int(EV_WRITE),
	)
	err := <-epoller.SubmitIoReq(ev)
	if err != nil {
		return ev.Withf(500, "epoller watch request failed : %v", err)
	}
	go c.run()
	return ev
}

// ===========================================================================
func (c *hdpWrite2) write1(b []byte) (int, error) {
	for range 3 {
		n, err := c.write(b)
		if err != nil {
			switch err {
			case unix.EAGAIN:
				<-time.After(100 * time.Millisecond)
				continue
			}
			return n, err
		}
		return n, nil
	}
	logger.Debugf("@@@@@@@@@@@@@@ %s exhausted EAGAIN write retries ...", c.cid)
	return 0, io.EOF
}

// ===========================================================================
func (c *hdpWrite2) writeFrame(req dtype.HdpEvent) {
	f := req.Frame()
	logger.Debugf("$$$$$$$$$$$ %s[%d] got a new write frame : %s $$$$$$$$$$$$$", c.cid, c.fd, f.B)
	err := <-waitIoReady(c.cid, c.fd, EV_WRITE)
	logger.Debugf("@@@@@@@@@@@@@@@@@ %s got io-ready result : %v", c.cid, err)
	f.N, err = c.write1(f.B)
	if err != nil {
		req.Ch() <- newHdpEvent(err)
		return
	}
	logger.Debugf("%s returning HDP_DATAGRAM write result : %d", c.cid, f.N)
	req.Ch() <- req.With(err, ":data", "byteNum", f.N)
}
