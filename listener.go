package aionet

import (
	"io"
	"os"
	"time"

	"github.com/pcbuildpluscoding/aionet/dtype"
	"golang.org/x/sys/unix"
)

// ==================================================================//
type HdpListener struct {
	*socket1
	connectTimeout time.Duration
	closeCh        chan error
	// recycleCh      chan recycleReq
}

// ===========================================================================
func (c *HdpListener) accept(chEv chan dtype.HdpEvent) {
	logger.Debugf("%s is accepting connections ...", c.cid)
	for ev := range c.tpt[C] {
		err := ev.Err()
		if err != nil {
			logger.Debugf("@@@@@@@@@@@ %s got an error : %v @@@@@@@@@@@@@@@@", c.cid, ev.Err())
			chEv <- ev
			return
		}
		switch ev.Flag1() {
		case dtype.HDP_ACCEPTED:
			chEv <- ev
			return
		default:
			c.handle(ev)
		}
	}
}

// ===========================================================================
func (c *HdpListener) Accept() (*HdpConn, error) {
	logger.Debugf("%s is accepting connections ...", c.cid)

	for {
		select {
		case <-time.After(c.connectTimeout):
			logger.Debugf("%s is ignoring accept timeout ...", c.cid)
		case err := <-c.submitReq(unix.EPOLL_CTL_MOD, unix.EPOLLIN, EV_READ):
			switch err {
			case io.ErrUnexpectedEOF, unix.EINTR:
				continue
			case nil:
				logger.Debugf("HDPListener accepted a connection ...")
				return c.onAccept()
			default:
				return nil, err
			}
		}
	}
}

// ==================================================================
func (c *HdpListener) onAccept() (*HdpConn, error) {
	ev := <-c.tpt.SendEvent(R, dtype.HDP_CONNECT).Sync()
	ev = <-c.tpt.SendEvent1(W, ev.With(dtype.HDP_CONNECT_ACK)).Sync()
	return nil, nil
}

// ==================================================================
func (c *HdpListener) ConnId() string {
	return c.cid
}

// ===========================================================================
func (c *HdpListener) handle(req dtype.HdpEvent) {
	logger.Debugf("%s is handling a request : %v ...", c.cid, req)
	switch flag := req.Flag1(); flag {
	case dtype.HDP_CONNECT:
		c.onInit(req)
	default:
		c.tpt <- req.Withf(500, "%s got unexpected event : %s", c.cid, flag.String())
	}
}

// ===========================================================================
func (c *HdpListener) onInit(req dtype.HdpEvent) {
	pipeName := req.String("init/dial/pipename")
	if pipeName == "" {
		c.tpt <- req.Withf(500, "init/dial/pipename is undefined")
		return
	}

	conn := NewHdpConn(dtype.HDP_ACCEPT)
	err := conn.Start(pipeName, req.With(dtype.HDP_INIT1))

	if err != nil {
		c.tpt <- req.With(err)
		return
	}

	c.tpt <- req.With(dtype.HDP_ACCEPTED, ":data", "hdpConn", conn)
}

// ===========================================================================
func (c *HdpListener) listen() (dtype.HdpEvent, error) {
	// don't need to lock here because AioListener is the exclusive reader of <l.connex.pname>.accept
	logger.Debugf("############## %s is listening : ##################", c.cid)
	ev := dtype.HdpEvent{}
	b, err := c.pread()
	if err != nil {
		logger.Debugf("############## got listener error : %v", err)
		return ev, err
	}
	logger.Debugf("############## got a message : ##################")
	return ev.Decode(b)
}

// ===========================================================================
func (c *HdpListener) receive() {
	for <-c.stopped == false {
		ev, err := c.listen()
		if err != nil {
			c.tpt <- ev.With(err)
			return
		}
		logger.Debugf("$$$$$$$$$$$ %s got a new event : %v $$$$$$$$$$$$$", c.cid, ev)
		c.tpt <- ev
	}
	logger.Debugf("%s listener is stopping ...", c.cid)
}

// ===========================================================================
func (c *HdpListener) run() {
	logger.Debugf("%s is starting with pipename %s ...", c.cid, c.pname)
	err := c.start1(0, os.O_RDONLY)
	if err != nil {
		logger.Errorf("%s got uniconn.start os.O_RDONLY error : %v", c.cid, err)
		return
	}
	logger.Debugf("############## %s is started : ##################", c.cid)
	c.stopped <- false
	go c.receive()
}
