package aionet

import (
	"fmt"
	"io"
	"time"

	"github.com/pcbuildpluscoding/aionet/dtype"
	"golang.org/x/sys/unix"
)

// ==================================================================//
type HdpListener0 struct {
	*socket
	cid            string
	connectTimeout time.Duration
	tpt            dtype.MultiCh
}

// ==================================================================//
type HdpListener struct {
	socket1
	connectTimeout time.Duration
	tpt            dtype.MultiCh
}

// ==================================================================
func (d *HdpListener) Cid() string {
	return d.cid
}

// ===========================================================================
func (c *HdpListener) Accept() (*HdpConn, error) {
	logger.Debugf("%s is accepting connections on %s ...", c.cid, c.laddr.String())

	for {
		select {
		case <-time.After(c.connectTimeout):
			logger.Debugf("%s is ignoring accept timeout ...", c.cid)
		case err := <-c.submitReq(c.cid, unix.EPOLL_CTL_MOD, unix.EPOLLIN, EV_READ):
			switch err {
			case io.ErrUnexpectedEOF, unix.EINTR:
				continue
			case nil:
				logger.Debugf("%s accepted a connection ...", c.cid)
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
	logger.Debugf("####### got first returned event : %v", ev)
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
func (c *HdpListener) start0() (*HdpListener, error) {
	logger.Debugf("%s is starting ...", c.cid)
	err := c.init(c.cid + "|" + c.cid)
	if err != nil {
		return nil, err
	}
	s, err := newSocket0("listen", unix.SOCK_DGRAM, 0, nil, nil)
	if err != nil {
		return nil, err
	}
	readyCh := make(chan bool, 1)
	refNum := [2]uint16{getRefNum(), 0}
	cidR := "acceptRead1-" + time.Now().Format("05.00000")
	s1 := s.newSocket1(cidR, 16)
	go newHdpRead1(s1, &refNum, c.tpt).run(readyCh)
	<-readyCh
	cidW := "acceptWrite1-" + time.Now().Format("05.00000")
	s1 = s.newSocket1(cidW, 16)
	go newHdpWrite1(s1, &refNum, c.tpt).run(readyCh)
	<-readyCh
	return c, s.init(cidR + "|" + cidW)
}

// ================================================================
func (c *HdpListener0) start1() (*HdpListener0, error) {
	logger.Debugf("%s is starting ...", c.cid)
	saddr, err := addrToSockaddr(c.laddr).sockaddr()
	if err != nil {
		return nil, err
	}
	err = unix.Bind(c.fd, saddr)
	if err != nil {
		return nil, fmt.Errorf("%s failed to bind to %s, error : %v", c.cid, c.laddr.String(), err)
	}
	readyCh := make(chan bool, 1)
	refNum := [2]uint16{getRefNum(), 0}
	cid := "acceptRead1-" + time.Now().Format("05.00000")
	s1 := c.newSocket1(cid, 16)
	go newHdpRead1(s1, &refNum, c.tpt).run(readyCh)
	<-readyCh
	return c, c.init(c.cid + "|" + c.cid)
}

// ================================================================
func (c *HdpListener) start() (*HdpListener, error) {
	logger.Debugf("%s is starting ...", c.cid)
	readyCh := make(chan bool, 1)
	refNum := [2]uint16{getRefNum(), 0}
	cid := "acceptRead1-" + time.Now().Format("05.00000")
	s1 := c.newSocket1(cid, 16)
	go newHdpRead1(s1, &refNum, c.tpt).run(readyCh)
	<-readyCh
	return c, c.bind(c.cid + "|" + c.cid)
}
