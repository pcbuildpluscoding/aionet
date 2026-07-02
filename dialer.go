package aionet

import (
	"time"

	"github.com/google/uuid"
	"github.com/pcbuildpluscoding/aionet/dtype"
)

// =================================================================//
// HdpDialer
// =================================================================//
type HdpDialer struct {
	cid string
}

// ==================================================================
func (d *HdpDialer) ConnId() string {
	return d.cid
}

// ==================================================================
func (d *HdpDialer) Dial(req dtype.HdpEvent) (*HdpConn, error) {
	logger.Debugf("%s is connecting to HdpListener ...", d.cid)

	d.cid = "hdpDialer-" + time.Now().Format("05.00000")
	hdpconn := NewHdpConn(dtype.HDP_DIAL)
	pipeName := uuid.New().String()

	err := hdpconn.Start(pipeName, req.With(dtype.HDP_CONNECT, ":data", "init/dial/pipename", pipeName))
	return hdpconn, err
}
