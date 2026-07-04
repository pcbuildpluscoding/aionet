package aionet

import (
	"io"
	"time"

	"github.com/pcbuildpluscoding/aionet/dtype"
	"golang.org/x/sys/unix"
)

// ==================================================================//
type HdpListener struct {
	*socket
	connectTimeout time.Duration
	tpt            dtype.MultiCh
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
	ev := <-c.tpt.SendEvent(R, dtype.HDP_ACCEPT).Sync()
	if ev.Err() != nil {
		return nil, ev.Err()
	}
	ev = <-c.tpt.SendEvent1(W, ev.With(dtype.HDP_CONNECT_ACK)).Sync()
	if ev.Err() != nil {
		return nil, ev.Err()
	}
	ev = <-c.tpt.SendEvent1(R, ev.With(dtype.HDP_ACCEPT_ACK)).Sync()
	if ev.Err() != nil {
		return nil, ev.Err()
	}
	ev = <-c.tpt.SendEvent1(W, ev.With(dtype.HDP_CONNECTED)).Sync()
	if ev.Err() != nil {
		return nil, ev.Err()
	}
	c.tpt.SendEvent1(R, ev.With(dtype.HDP_OPEN1)).Async()
	c.tpt.SendEvent1(W, ev.With(dtype.HDP_OPEN1)).Async()
	return &HdpConn{
		cid: "hdpConn-" + time.Now().Format("05.00000"),
		tpt: c.tpt,
	}, nil
}

// ================================================================
func (c *HdpListener) start() *HdpListener {
	logger.Debugf("%s is starting ...", c.cid)
	readyCh := make(chan bool, 1)
	refNum := [2]uint16{getRefNum(), 0}
	s1 := c.newSocket1()
	go newHdpRead1(s1, &refNum, c.tpt).run(readyCh)
	<-readyCh
	go newHdpWrite1(s1, &refNum, c.tpt).run(readyCh)
	<-readyCh
	return c
}
