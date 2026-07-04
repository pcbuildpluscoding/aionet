package aionet

import (
	"encoding/binary"
	"time"

	"github.com/howeyc/crc16"
	"github.com/pcbuildpluscoding/aionet/dtype"
	"golang.org/x/sys/unix"
)

// ===========================================================================
type hdpRead1 struct {
	socket1
	refNum *[2]uint16
	tpt    dtype.MultiCh
}

// ===========================================================================
func (c *hdpRead1) handle(req dtype.HdpEvent) {
	logger.Debugf("%s is handling a request : %v ...", c.cid, req)
	f := func() dtype.HdpEvent {
		switch flag := req.Flag1(); flag {
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
			return req.Withf(500, "unknown flag : %s", flag.String())
		}
	}
	req.Respond(f())
}

// ===========================================================================
func (c *hdpRead1) onAccept(res dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is reading initial client connection  ...", c.cid)
	// read accept acknowledgement
	b := make([]byte, 14)
	// read the serviceConn address
	var err error
	n, raddr, err := c.onReadFrom(c.cid, b, 0)
	if err != nil {
		err = NewHdpError(ErrReadFromNewConn, c.cid, "onConnect", n, err)
		return res.With(err)
	}

	// verify checksum
	err = verifyChecksum1(c.cid, b)
	if err != nil {
		err = NewHdpError(ErrUnequalChecksum, c.cid, "onConnect", err)
		return res.With(err)
	}

	flag := dtype.HDP_STATE1(b[6])
	// verify protocol correctness
	if flag != dtype.HDP_CONNECT {
		// TODO - report the error to remotePeer
		err = NewHdpError(ErrUnexpectedReadFlag, c.cid, "onConnect", flag.String())
		return res.With(err)
	}

	return res.With(err, ":data", "raddr", raddr)
}

// ===========================================================================
func (c *hdpRead1) onAcceptAcknow(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is reading client accept acknowledgement ...", c.cid)
	// read accept acknowledgement
	b := make([]byte, 14)
	err := c.readHeader(b)
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
	if flag != dtype.HDP_ACCEPT_ACK {
		// TODO - report the error to remotePeer
		return req.With(NewHdpError(ErrUnexpectedReadFlag, c.cid, "onAcceptAcknow", flag.String()))
	}
	c.windowSize = binary.LittleEndian.Uint16(b[8:10])
	// logger.Debugf("%s got window size in state HDP_ACCEPT_ACK : %d", c.cid, wsize)
	// res.data = wsize

	// logger.Debugf("%s service is connected ...", c.cid)
	return req
}

// ===========================================================================
func (c *hdpRead1) onConnect(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is reading initial client connection  ...", c.cid)
	err := func() error {
		// read accept acknowledgement
		b := make([]byte, 14)
		// read the serviceConn address
		var err error
		n, raddr, err := c.onReadFrom(c.cid, b, 0)
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
func (c *hdpRead1) onConnectAcknow(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is reading peer connect acknowledgement ...", c.cid)
	var wsize uint16
	err := func() error {
		hdr := make([]byte, 14)
		// logger.Debugf("%s parseHeader is running ...", c.cid)
		err := c.readHeader(hdr)
		if err != nil {
			return err
		}

		refNum := binary.LittleEndian.Uint16(hdr[:2])

		if c.refNum[0] != 0 && c.refNum[0] != refNum {
			// reject the request and respond to the remotePeer
			logger.Debugf("######## %s got wrong peer refnum : %d, %d", c.cid, c.refNum[0], refNum)
			return NewHdpError(ErrCodeWrongPeerRefNum)
		}

		err = verifyChecksum1(c.cid, hdr)
		if err != nil {
			return NewHdpError(ErrUnequalChecksum, c.cid, "parseHeader", err)
		}

		flag := dtype.HDP_STATE1(hdr[6])
		// verify protocol correctness
		if flag != dtype.HDP_CONNECT_ACK {
			// TODO - report the error to remotePeer
			return NewHdpError(ErrUnexpectedReadFlag, c.cid, "onConnectAcknow", flag.String())
		}

		c.refNum[1] = binary.LittleEndian.Uint16(hdr[:2])
		logger.Debugf("####### %s onConnectAcknow setting readHDP.refNum from peer exchange : %d", c.cid, c.refNum)
		// logger.Debugf("%s got new session refNum : %d", c.cid, c.refNum)

		c.windowSize = binary.LittleEndian.Uint16(hdr[8:10])
		logger.Debugf("%s got window size in state HDP_CONNECT_ACK : %d", c.cid, wsize)
		return nil
	}
	return req.With(err)
}

// ===========================================================================
func (c *hdpRead1) onOpenAcknow(res dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is reading open status acknowlegement ...", c.cid)
	b := make([]byte, 14)
	err := c.readHeader(b)
	if err != nil {
		return res.With(err)
	}

	// verify checksum
	err = verifyChecksum1(c.cid, b)
	if err != nil {
		err = NewHdpError(ErrUnequalChecksum, c.cid, "onOpenAcknow", err)
		return res.With(err)
	}

	refNum := binary.LittleEndian.Uint16(b[:2])

	logger.Debugf("%s got refNum onOpenAcknow : %d, %d", c.cid, refNum, c.refNum[0])

	if c.refNum[0] != 0 && c.refNum[0] != refNum {
		// reject the request and respond to the remotePeer
		err = NewHdpError(ErrCodeWrongPeerRefNum)
		return res.With(err)
	}

	flag := dtype.HDP_STATE1(b[6])

	logger.Debugf("%s in openAcknow got flag : %s", c.cid, flag.String())
	// verify protocol correctness
	if flag != dtype.HDP_CONNECTED {
		// TODO - report the error to remotePeer
		err = NewHdpError(ErrUnexpectedReadFlag, c.cid, "onOpenAcknow", flag.String())
		return res.With(err)
	}

	logger.Debugf("%s remote connection is now open ...", c.cid)
	return res
}

// ===========================================================================
func (c *hdpRead1) newHdpRead2() *hdpRead2 {
	conn := &hdpRead2{
		socket: c.socket,
		refNum: c.refNum,
		tpt:    c.tpt,
	}
	return conn
}

// ===========================================================================
func (c *hdpRead1) parseHeader() ([]byte, error) {
	h := make([]byte, 14)
	// logger.Debugf("%s parseHeader is running ...", c.cid)
	err := c.readHeader(h)
	if err != nil {
		return h, err
	}

	refNum := binary.LittleEndian.Uint16(h[:2])

	if c.refNum[0] != 0 && c.refNum[0] != refNum {
		// reject the request and respond to the remotePeer
		logger.Debugf("######## %s got wrong peer refnum : %d, %d", c.cid, c.refNum[0], refNum)
		return h, NewHdpError(ErrCodeWrongPeerRefNum)
	}

	err = verifyChecksum1(c.cid, h)
	if err != nil {
		return h, NewHdpError(ErrUnequalChecksum, c.cid, "parseHeader", err)
	}

	return h, nil
}

// ===========================================================================
func (c *hdpRead1) readHeader(b []byte) error {
	// logger.Debugf("%s is wanting read readiness ...", c.cid)
	err := <-c.submitReq(c.cid, unix.EPOLL_CTL_MOD, unix.EPOLLIN, EV_READ)
	if err != nil {
		return err
	}
	logger.Debugf("%s got read-readiness ...", c.cid)
	for nn := 0; nn < len(b); {
		n, err := c.read(b[nn:])
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
func (c *hdpRead1) run(readyCh chan bool) {
	logger.Debugf("%s is running ...", c.cid)
	readyCh <- true
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
			c.newHdpRead2().run()
			return
		default:
			c.handle(ev)
		}
	}
}

// ===========================================================================
type hdpWrite1 struct {
	socket1
	refNum *[2]uint16
	tpt    dtype.MultiCh
}

// ===========================================================================
func (c *hdpWrite1) handle(req dtype.HdpEvent) {
	logger.Debugf("%s is handling a request : %v ...", c.cid, req)
	f := func() dtype.HdpEvent {
		switch req.Flag1() {
		case dtype.HDP_CONNECT_ACK:
			return c.acknowConnect(req)
		case dtype.HDP_ACCEPT_ACK:
			return c.acknowAccept(req)
		case dtype.HDP_CONNECTED:
			return c.acknowOpen(req)
		case dtype.HDP_CONNECT:
			return c.connectHdp(req)
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
func (c *hdpWrite1) acknowConnect(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is acknowledging connected peer ...", c.cid)

	if req.Addr("raddr") == nil {
		return req.Withf(500, "peer conn remote address is undefined")
	}
	logger.Debugf("%s is making a transport connection to remote address : %s ... ", c.cid, req.Addr("raddr").String())
	dura := time.Duration(5) * time.Second
	err := c.connect(&sockAddr{Addr: req.Addr("raddr")}, dura)
	if err != nil {
		err = NewHdpError(ErrConnectToNewConn, c.cid, "acknowConnect", err)
		return req.With(err)
	}

	// for an 18 hour session period
	logger.Debugf("######### %s acknowConnect setting hdpWrite.refNum : %d", c.cid, c.refNum)
	// logger.Debugf("%s refNum is created : %d", c.cid, c.refNum)
	b := make([]byte, 14)
	binary.LittleEndian.PutUint16(b[0:2], c.refNum[0])

	// set the HDP_CONNECT_ACK flags
	b[6] = byte(dtype.HDP_CONNECT_ACK)

	// set the local window size value
	wsize := req.UInt16("windowSize")
	// logger.Debugf("%s is sending window size in state HDP_CONNECT_ACK : %d", c.cid, wsize)
	binary.LittleEndian.PutUint16(b[8:10], wsize)

	// insert the checksum
	binary.LittleEndian.PutUint16(b[12:], crc16.Checksum(b, crc16.IBMTable))

	// logger.Debugf("%s hdpWrite is writing the acknowConnect header ...", c.cid)
	return req.With(c.writeHeader(b))
}

// ===========================================================================
func (c *hdpWrite1) acknowOpen(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s hdpWrite is acknowledging open status ...", c.cid)

	b := make([]byte, 14)
	binary.LittleEndian.PutUint16(b[0:2], c.refNum[1])

	// set the FLG_ACK flags
	b[6] = byte(dtype.HDP_CONNECTED)

	// calculate and insert the checksum
	binary.LittleEndian.PutUint16(b[12:], crc16.Checksum(b, crc16.IBMTable))

	err := c.writeHeader(b)
	logger.Debugf("%s hdpWrite remote connection is now open ...", c.cid)
	return req.With(err)
}

// ===========================================================================
func (c *hdpWrite1) connectTo(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is connecting to peer ...", c.cid)

	// logger.Debugf("%s refNum is created : %d", c.cid, c.refNum)
	b := make([]byte, 14)
	binary.LittleEndian.PutUint16(b[0:2], c.refNum[0])

	// set the HDP_CONNECT_ACK flags
	b[6] = byte(dtype.HDP_CONNECT)

	// set the local window size value
	wsize := req.UInt16("windowSize")
	// logger.Debugf("%s is sending window size in state HDP_CONNECT_ACK : %d", c.cid, wsize)
	binary.LittleEndian.PutUint16(b[8:10], wsize)

	// insert the checksum
	binary.LittleEndian.PutUint16(b[12:], crc16.Checksum(b, crc16.IBMTable))

	// logger.Debugf("%s hdpWrite is writing the acknowConnect header ...", c.cid)
	return req.With(c.writeHeader(b))
}

// ===========================================================================
func (c *hdpWrite1) connectHdp(req dtype.HdpEvent) dtype.HdpEvent {
	logger.Debugf("%s is connecting to peer ...", c.cid)
	b := make([]byte, 14)
	// set the HDP_CONNECT_ACK flags
	b[6] = byte(dtype.HDP_CONNECT)
	// set the local window size value

	// calculate and insert the checksum
	// logger.Debugf("%s hdpWrite is connecting to HDPListener at %s", c.cid, req.Addr().String())
	binary.LittleEndian.PutUint16(b[12:], crc16.Checksum(b, crc16.IBMTable))

	logger.Debugf("%s hdpWrite connecting to remote address : %s", c.cid, req.Addr("raddr").String())
	n, err := c.onWriteTo(c.cid, b, req.Addr("raddr"))
	if err != nil {
		err = NewHdpError(ErrWriteToNewConn, c.cid, "connectHDP", n, err)
	}
	return req.With(err)
}

// ===========================================================================
func (c *hdpWrite1) newHdpWrite2() *hdpWrite2 {
	conn := &hdpWrite2{
		socket: c.socket,
		refNum: c.refNum,
		tpt:    c.tpt,
	}
	return conn
}

// ===========================================================================
func (c *hdpWrite1) run(readyCh chan bool) {
	logger.Debugf("%s is running ...", c.cid)
	readyCh <- true
	for ev := range c.tpt[W] {
		err := ev.Err()
		if err != nil {
			logger.Debugf("@@@@@@@@@@@ %s got an error : %v @@@@@@@@@@@@@@@@", c.cid, ev.Err())
			return
		}
		switch ev.Flag1() {
		case dtype.HDP_OPEN1:
			logger.Debugf("@@@@@@@@@@@@@@@ %s got a HDP_RESET1 event @@@@@@@@@@@@@@@@", c.cid)
			c.newHdpWrite2().run()
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
	err := <-c.submitReq(c.cid, unix.EPOLL_CTL_MOD, unix.EPOLLOUT, EV_WRITE)
	if err != nil {
		return err
	}
	for nn := 0; nn < len(b); {
		n, err := c.write(b[nn:])
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
