package aionet

import (
	"io"
	"time"

	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/aionet/epoller"
	"golang.org/x/sys/unix"
)

// ===========================================================================
type hdpRead2 struct {
	*socket
	cid    string
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
		"reqRef/flags", int(unix.EPOLLIN),
		"reqRef/deadline", req.Value("deadline")))
	req.Ch() <- req.With(err)
}

// ===========================================================================
type hdpWrite2 struct {
	*socket
	ackTimeout time.Duration
	buffer     BufferW
	cid        string
	rb         *ringBuffer
	refNum     *[2]uint16
	// state      [2]dtype.HDP_STATE2
	tpt dtype.MultiCh
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
func (c *hdpWrite2) putFrame(req dtype.HdpEvent) {
	f := func() (int, error) {
		if c.buffer.isFull() {
			logger.Debugf("%s buffer is full : %v", c.cid, req)
			return 0, VErrEOF
		}
		logger.Debugf("%s getting next seqNum ...", c.cid)
		seqNum, err := c.rb.nextSeqNum()
		if err != nil {
			return 0, err
		}
		return c.buffer.addEntry(req.Bytes(), seqNum, req.UInt32("timerKey"), c.cid)
	}
	// flag := dtype.HDP_WRITE2
	n, err := f()
	if err != nil {
		// in future this might change to HDP_ESCALATE and be referred to the control conn
		// flag = dtype.HDP_RESET
	}
	// send the buffered frame length back to the HdpConn eventloop
	logger.Debugf("%s returning HDP_DATAGRAM write result : %d", c.cid, n)
	req.Ch() <- req.With(err, ":data", "byteNum", n)
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
		switch ev.Flag2() {
		case dtype.HDP_DATAGRAM:
			if ev.Value("deadline") == nil {
				c.writeFrame(ev)
				continue
			}
			c.setDeadline(ev)
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
		"reqRef/flags", int(unix.EPOLLOUT),
		"reqRef/deadline", req.Value("deadline")))
	req.Ch() <- req.With(err)
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
