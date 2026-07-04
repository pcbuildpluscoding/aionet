package aionet

import (
	"net"

	"github.com/pcbuildpluscoding/aionet/dtype"
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
func (d *HdpDialer) Dial(network, addr string) (*HdpConn, error) {
	logger.Debugf("%s is connecting to HdpListener ...", d.cid)

	raddr, err := net.ResolveUDPAddr(network, addr)
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
