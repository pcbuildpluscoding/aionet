package aionet

import (
	"net"

	"github.com/pcbuildpluscoding/aionet/dtype"
	"golang.org/x/sys/unix"
)

// =================================================================//
// HdpDialer
// =================================================================//
type HdpDialer struct {
	cid string
	tpt dtype.MultiCh
}

// ==================================================================
func (d *HdpDialer) Cid() string {
	return d.cid
}

// ==================================================================
func (d *HdpDialer) Dial(network, address string) (*HdpConn, error) {
	logger.Debugf("%s is connecting to HdpListener ...", d.cid)

	raddr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	ev := <-d.tpt.SendEvent1(W, newHdpEvent(dtype.HDP_CONNECT, ":data",
		"windowSize", 16,
		"raddr", raddr)).Sync()
	if ev.Err() != nil {
		return nil, ev.Err()
	}
	ev = <-d.tpt.SendEvent1(R, newHdpEvent(dtype.HDP_CONNECT_ACK)).Sync()
	if ev.Err() != nil {
		return nil, ev.Err()
	}
	ev = <-d.tpt.SendEvent1(W, newHdpEvent(dtype.HDP_ACCEPT_ACK)).Sync()
	if ev.Err() != nil {
		return nil, ev.Err()
	}
	ev = <-d.tpt.SendEvent1(R, newHdpEvent(dtype.HDP_CONNECTED)).Sync()
	if ev.Err() != nil {
		return nil, ev.Err()
	}

	d.tpt.SendEvent1(R, ev.With(dtype.HDP_OPEN1)).Async()
	d.tpt.SendEvent1(W, ev.With(dtype.HDP_OPEN1)).Async()
	return &HdpConn{
		cid: "hdpConn-",
		tpt: d.tpt,
	}, nil
}

// ================================================================
func (d *HdpDialer) start() (*HdpDialer, error) {
	logger.Debugf("%s is starting ...", d.cid)
	s, err := newSocket("dial", unix.SOCK_DGRAM, 0, nil, nil)
	if err != nil {
		return nil, err
	}
	err = s.init()
	if err != nil {
		return nil, err
	}
	s1 := s.newSocket1()
	readyCh := make(chan bool, 1)
	refNum := [2]uint16{getRefNum(), 0}
	go newHdpRead1(s1, &refNum, d.tpt).run(readyCh)
	<-readyCh
	go newHdpWrite1(s1, &refNum, d.tpt).run(readyCh)
	<-readyCh
	return d, nil
}
