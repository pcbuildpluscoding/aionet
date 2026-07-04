package aionet

import "github.com/pcbuildpluscoding/aionet/dtype"

// ===========================================================================
type hdpRead2 struct {
	*socket
	refNum *[2]uint16
	tpt    dtype.MultiCh
}

// ===========================================================================
func (c *hdpRead2) run() {
	logger.Debugf("%s is running ...", c.cid)
	for ev := range c.tpt[R] {
		err := ev.Err()
		if err != nil {
			logger.Debugf("@@@@@@@@@@@ %s got an error : %v @@@@@@@@@@@@@@@@", c.cid, ev.Err())
			return
		}
		switch ev.Flag1() {
		case dtype.HDP_OPEN1:
			logger.Debugf("@@@@@@@@@@@@@@@ %s got a HDP_OPEN1 event @@@@@@@@@@@@@@@@", c.cid)
			return
		default:
			// c.handle(ev)
		}
	}
}

// ===========================================================================
type hdpWrite2 struct {
	*socket
	refNum *[2]uint16
	tpt    dtype.MultiCh
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
		switch ev.Flag1() {
		case dtype.HDP_OPEN1:
			logger.Debugf("@@@@@@@@@@@@@@@ %s got a HDP_OPEN1 event @@@@@@@@@@@@@@@@", c.cid)
			return
		default:
			// c.handle(ev)
		}
	}
}
