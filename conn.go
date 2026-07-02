package aionet

import (
	"fmt"
	"time"

	"github.com/pcbuildpluscoding/aionet/dtype"
)

// ===========================================================================
type HdpConn struct {
	cid    string
	kind   dtype.HDP_STATE1
	state  [2]dtype.HDP_STATE2
	statet Statet
	tpt    dtype.MultiCh
}

// ===========================================================================
func (c *HdpConn) Cid() string {
	return c.cid
}

// ===========================================================================
func (c *HdpConn) Read(b []byte) (int, error) {
	res := <-c.tpt.SendEvent(R, dtype.HDP_DATAGRAM1, ":data", "bytes", b).Sync()
	logger.Debugf("%s got HDP_DATAGRAM read result : %v", c.cid, res)
	return res.Retval()
}

// ===========================================================================
func (c *HdpConn) Write(b []byte) (int, error) {
	res := <-c.tpt.SendEvent(W, dtype.HDP_DATAGRAM1, ":data", "frame", dtype.NewFrame(b)).Sync()
	logger.Debugf("%s got HDP_DATAGRAM write result : %v", c.cid, res)
	return res.Retval()
}

// ===========================================================================
func (c *HdpConn) recycle(ev dtype.HdpEvent) {
	f1 := c.statet
	// reset c.statet so that if a handler does not assign a value
	// then f2 is finally assigned to it.
	c.statet = nil
	var f2 Statet
	for f1 != nil {
		f2 = f1
		f1, ev = f2(ev)
	}
	if c.statet == nil {
		c.statet = f2
	}
	logger.Debugf("%s got final event result : %v", c.cid, ev)
}

// ===========================================================================
func (c *HdpConn) Run() {
	logger.Debugf("%s eventloop is running ...", c.cid)
	// c.statet = c.handleOpen
	for ev := range c.tpt[C] {
		logger.Debugf("############## %s control got another event %v", c.cid, ev)
		c.recycle(ev)
		switch c.state[0] {
		case dtype.HDP_CLOSED:
			logger.Debugf("%s is now closed.", c.cid)
			return
		}
	}
}

// ===========================================================================
func (c *HdpConn) SetReadDeadline(dl time.Time) error {
	res := <-c.tpt.SendEvent(R, dtype.HDP_DATAGRAM1, ":data", "deadline", dl).Sync()
	logger.Debugf("%s got setReadDeadline result : %v", c.cid, res)
	return res.Err()
}

// ===========================================================================
func (c *HdpConn) SetWriteDeadline(dl time.Time) error {
	res := <-c.tpt.SendEvent(W, dtype.HDP_DATAGRAM1, ":data", "deadline", dl).Sync()
	logger.Debugf("%s got setWriteDeadline result : %v", c.cid, res)
	return res.Err()
}

// ===========================================================================
func (c *HdpConn) Start(pipeName string, ev dtype.HdpEvent) error {

	logger.Debugf("$$$$$$$$$$$$$ %s got starting event : %v", c.cid, ev)
	c.tpt[R] <- ev
	switch c.kind {
	case dtype.HDP_ACCEPT:
		go NewInitAcceptRead(pipeName, c.tpt).Run()
		go NewInitAcceptWrite(pipeName, c.tpt).Run()
	case dtype.HDP_DIAL:
		go NewInitDialRead(pipeName, c.tpt).Run()
		go NewInitDialWrite(pipeName, c.tpt).Run()
	}

	select {
	case err := <-c.start():
		return err
	case <-time.After(10 * time.Second):
		return fmt.Errorf("%s starting timedout", c.cid)
	}
}

// ===========================================================================
func (c *HdpConn) start() chan error {
	ch := make(chan error, 1)
	go func() {
		for range 2 {
			ev := <-c.tpt[C]
			if ev.Err() != nil {
				ch <- ev.Err()
				return
			}
		}
		ch <- nil
	}()
	return ch
}
