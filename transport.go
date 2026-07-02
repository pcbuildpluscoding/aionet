package aionet

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/howeyc/crc16"
	"github.com/pcbuildpluscoding/aionet/dtype"
	"golang.org/x/sys/unix"
)

// ===========================================================================
type hdpRead1 struct {
	*socket1
	refNum *[2]uint16
}

// ===========================================================================
func (c *hdpRead1) handle(req dtype.HdpEvent) {
	// logger.Debugf("%s is handling a request ...", c.cid)
	f := func() dtype.HdpEvent {
		switch req.Flag1() {
		case dtype.HDP_ACCEPT:
			return c.onAccept(req)
		case dtype.HDP_CONNECT:
			return c.onConnect(req)
		case dtype.HDP_CONNECT_ACK:
			return c.onConnectAcknow(req)
		case dtype.HDP_ACCEPT_ACK:
			return c.onAcceptAcknow(req)
		case dtype.HDP_CONNECTED:
			return c.onOpenAcknow(req)
		case dtype.HDP_OPEN1:
			return req
		default:
			return c.parseHeader()
		}
	}
	req.Respond(f())
}

// ===========================================================================
func (c *hdpRead1) onAcceptAcknow(req dtype.HdpEvent) dtype.HdpEvent {
	// logger.Debugf("%s is reading client accept acknowledgement ...", c.cid)
	// read accept acknowledgement
	b := make([]byte, 14)
	err := c.read1(b)
	if err != nil {
		return req.With(err)
	}

	// verify checksum
	err = verifyChecksum1(c.cid, b)
	if err != nil {
		return req.With(NewHdpError(ErrUnequalChecksum, c.cid, "onAcceptAcknow", err))
	}

	refNum := binary.LittleEndian.Uint16(b[0:2])
	c.refNum[1] = binary.LittleEndian.Uint16(b[2:4])

	logger.Debugf("$$$$$$ %s onAcceptAcknow setting readHDP.refNum[1] from peer exchange, checking refNum[0] local, remote : %d, %d", c.cid, c.refNum, refNum)

	if c.refNum[0] != 0 && c.refNum[0] != refNum {
		// reject the request and respond to the remotePeer
		return req.With(NewHdpError(ErrCodeWrongPeerRefNum))
	}

	flag := dtype.HDP_STATE1(b[6])

	// verify protocol correctness
	if flag == dtype.HDP_ACCEPT_ACK {
		// TODO - report the error to remotePeer
		return req.With(NewHdpError(ErrUnexpectedReadFlag, c.cid, "onAcceptAcknow", flag.String()))
	}
	wsize := binary.LittleEndian.Uint16(b[8:10])
	// logger.Debugf("%s got window size in state HDP_ACCEPT_ACK : %d", c.cid, wsize)
	// res.data = wsize

	// logger.Debugf("%s service is connected ...", c.cid)
	return req.With("Data:", "windowSize", wsize)
}

// ===========================================================================
func (c *hdpRead1) onConnect(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is reading initial client connection  ...", c.cid)
	err := func() error {
		// read accept acknowledgement
		b := make([]byte, 14)
		// read the serviceConn address
		var err error
		n, raddr, err := c.onReadFrom(b, 0)
		if err != nil {
			return NewHdpError(ErrReadFromNewConn, c.cid, "onConnect", n, err)
		}

		// verify checksum
		err = verifyChecksum1(c.cid, b)
		if err != nil {
			return NewHdpError(ErrUnequalChecksum, c.cid, "onConnect", err)
		}

		flag := dtype.HDP_STATE1(b[6])
		// verify protocol correctness
		if flag != dtype.HDP_CONNECT {
			// TODO - report the error to remotePeer
			return NewHdpError(ErrUnexpectedReadFlag, c.cid, "onConnect", flag.String())
		}

		// logger.Debugf("%s is making a transport connection to remote address : %s ... ", c.cid, raddr.String())
		dura := time.Duration(5) * time.Second
		err = c.connect(&sockAddr{Addr: raddr}, dura)
		if err != nil {
			return NewHdpError(ErrConnectToNewConn, c.cid, "onConnect", err)
		}
		return nil
	}
	return req.With(err)
}

// ===========================================================================
func (c *hdpRead1) newHdpRead2() *hdpRead2 {
	cid := "acceptRead1-" + time.Now().Format("05.00000")
	conn := &hdpRead2{
		tpt: c.tpt,
	}
	conn.fd = c.fd
	return conn
}

// ===========================================================================
// parseHeader
// - step1: confirm connRefNum is matching
// - step2: check the new recv-seqnum that it is in range
// - step3: check the control-bits
// ===========================================================================
func (c *hdpRead1) parseHeader() (res dtype.HdpEvent) {
	h := make([]byte, 14)
	// logger.Debugf("%s parseHeader is running ...", c.cid)
	err := c.read1(h)
	if err != nil {
		return res.With(err)
	}

	refNum := binary.LittleEndian.Uint16(h[:2])

	if c.refNum[0] != 0 && c.refNum[0] != refNum {
		// reject the request and respond to the remotePeer
		logger.Debugf("######## %s got wrong peer refnum : %d, %d", c.cid, c.refNum[0], refNum)
		return res.With(NewHdpError(ErrCodeWrongPeerRefNum))
	}

	err = verifyChecksum1(c.cid, h)
	if err != nil {
		return res.With(NewHdpError(ErrUnequalChecksum, c.cid, "parseHeader", err))
	}

	flag := dtype.HDP_STATE2(h[6])

	var data []byte
	switch flag {
	case dtype.HDP_DATA_ACK:
		logger.Debugf("%s got data-acknowledgement for seqNum : %d", c.cid, binary.LittleEndian.Uint32(h[2:6]))
		// set the acknowledged seqNum for ringBuffer removal, plus the mode bit
		// note - this event will only happen in controlHDP scope, not in HDPConn scope
		// logger.Debugf("readHDP-%d got HDP_DATA_ACK frame with seqNum : %d", c.id, binary.LittleEndian.Uint32(h[2:6]))
		data = append(h[2:6], byte(0))
	case dtype.HDP_DATAGRAM:
		// set the remote seqNum for controlHDP to acknowledge, plus to mode bit
		// and append the data size and checksum
		logger.Debugf("%s got HDP_DATAGRAM frame with seqNum : %d", c.cid, binary.LittleEndian.Uint32(h[2:6]))
		data = append(append(h[2:6], byte(1)), h[8:12]...)
	}
	return res.With(flag, "Data:", "bytes", data)
}

// ===========================================================================
func (c *hdpRead1) read1(b []byte) error {
	for nn := 0; nn < len(b); {
		n, err := c.onRead(b[nn:])
		if n > 0 {
			nn += n
		}
		if err != nil {
			// logger.Errorf("%s readHDP read header error : %v", c.cid, err)
			return err
		}
	}
	return nil
}

// ===========================================================================
func (c *hdpRead1) receive() {
	for <-c.stopped == false {
		ev, err := c.listen()
		logger.Debugf("$$$$$$$$$$$ got a new event : %v $$$$$$$$$$$$$", ev)
		if err != nil {
			switch err {
			case io.EOF:
				logger.Debugf("$$$$$$$$$$$ %s got EOF error $$$$$$$$$$$$$", c.cid)
			case unix.EAGAIN:
				logger.Debugf("$$$$$$$$$$$ %s got EAGAIN error $$$$$$$$$$$$$", c.cid)
				<-time.After(100 * time.Millisecond)
				c.stopped <- false
				continue
			default:
				logger.Debugf("%s got listening error : %v", c.cid, err)
			}
			c.tpt[R] <- ev.With(err)
			return
		}
		c.tpt[R] <- ev
	}
	logger.Debugf("%s listener has stopped ...", c.cid)
}

// ===========================================================================
func (c *hdpRead1) Run() {
	logger.Debugf("%s is running ...", c.cid)
	for ev := range c.tpt[R] {
		err := ev.Err()
		if err != nil {
			logger.Debugf("@@@@@@@@@@@ %s got an error : %v @@@@@@@@@@@@@@@@", c.cid, ev.Err())
			return
		}
		switch ev.Flag1() {
		case dtype.HDP_OPEN1:
			logger.Debugf("@@@@@@@@@@@@@@@ %s got a HDP_RESET1 event @@@@@@@@@@@@@@@@", c.cid)
			// c.tpt[R] <- NewHdpEvent(dtype.HDP_INIT1)
			c.tpt[C] <- c.newHdpRead2().Start()
			return
		default:
			c.handle(ev)
		}
	}
}

// ===========================================================================
type hdpWrite1 struct {
	*socket1
	refNum *[2]uint16
}

// ===========================================================================
func (c *hdpWrite1) handle(req dtype.HdpEvent) {
	// logger.Debugf("%s hdpWrite is handling a request ...", c.cid)
	f := func() dtype.HdpEvent {
		switch req.Flag1() {
		case dtype.HDP_CONNECT_ACK:
			return c.acknowConnect(req)
		case dtype.HDP_ACCEPT_ACK:
			return c.acknowAccept(req)
		case dtype.HDP_CONNECTED:
			return c.acknowOpen(req)
		case dtype.HDP_CONNECT:
			return c.connectHDP(req)
		case dtype.HDP_OPEN1:
			return req
		// case HDP_TESTING:
		// 	return c.writeTest(req)
		default:
			return c.write1(req)
		}
	}
	req.Respond(f())
}

// ===========================================================================
func (c *hdpWrite1) acknowAccept(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is acknowledging connected acceptance, local refNum : %d ...", c.cid, c.refNum)

	b := make([]byte, 14)
	binary.LittleEndian.PutUint16(b[0:2], c.refNum[1])
	binary.LittleEndian.PutUint16(b[2:4], c.refNum[0])

	b[6] = byte(dtype.HDP_ACCEPT_ACK)
	// set the local window size value

	// binary.LittleEndian.PutUint16(b[10:12], c.wsize)
	wsize := req.UInt16("windowSize")
	// logger.Debugf("%s is sending window size in state HDP_ACCEPT_ACK : %d", c.cid, wsize)
	binary.LittleEndian.PutUint16(b[8:10], wsize)

	// calculate and insert the checksum
	binary.LittleEndian.PutUint16(b[12:], crc16.Checksum(b, crc16.IBMTable))

	// logger.Debugf("%s is writing the header ...", c.cid)
	err := c.writeHeader(b)
	return req.With(err)
}

// ===========================================================================
func (c *hdpWrite1) newHdpWrite2() *hdpWrite2 {
	cid := "hdpWrite2-" + time.Now().Format("05.00000")
	conn := &hdpWrite2{
		tpt: c.tpt,
	}
	conn.fd = c.fd
	return conn
}

// ===========================================================================
func (c *hdpWrite1) Run() {
	logger.Debugf("%s is running ...", c.cid)
	for ev := range c.tpt[W] {
		err := ev.Err()
		if err != nil {
			logger.Debugf("@@@@@@@@@@@ %s got an error : %v @@@@@@@@@@@@@@@@", c.cid, ev.Err())
			return
		}
		switch ev.Flag1() {
		case dtype.HDP_RESET1:
			logger.Debugf("@@@@@@@@@@@@@@@ %s got a HDP_RESET1 event @@@@@@@@@@@@@@@@", c.cid)
			// c.tpt[W] <- NewHdpEvent(dtype.HDP_INIT1)
			c.tpt[C] <- c.newHdpWrite2().Start()
			return
		default:
			c.handle(ev)
		}
	}
}

// ===========================================================================
func (c *hdpWrite1) write1(req dtype.HdpEvent) dtype.HdpEvent {
	hdr := req.Bytes()
	// logger.Debugf("%s got write request : %s", c.cid, frame)

	if len(hdr) == 0 {
		hdr = make([]byte, 14)
	}

	binary.LittleEndian.PutUint16(hdr[0:2], c.refNum[1])
	// create and set the initial local sequence number

	// set the FLG_ACK flags
	hdr[6] = byte(req.Flag1()) // FLG_ACK

	// insert the header checksum
	// crc := crc16.Checksum(hdr, crc16.IBMTable)
	// logger.Debugf("%s is sending header with checksum : %d", c.cid, crc)
	binary.LittleEndian.PutUint16(hdr[12:], crc16.Checksum(hdr, crc16.IBMTable))

	return req.With(c.writeHeader(hdr))
}

// ---------------------------------------------------------------//
// writeHeader
// ---------------------------------------------------------------//
func (c *hdpWrite1) writeHeader(b []byte) error {
	for nn := 0; nn < len(b); {
		n, err := c.onWrite(b[nn:])
		if n > 0 {
			nn += n
		}
		if err != nil {
			// logger.Errorf("%s write header error : %v", c.cid, err)
			return err
		}
	}
	return nil
}
