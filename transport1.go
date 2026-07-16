package aionet

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/howeyc/crc16"
	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/aionet/epoller"
	"golang.org/x/sys/unix"
)

// ===========================================================================
type hdpRead2 struct {
	*socket
	buffer BufferR
	cid    string
	refNum *[2]uint16
	tpt    dtype.MultiCh
}

// ===========================================================================
func (c *hdpRead2) handle(req dtype.HdpEvent) {
	logger.Debugf("%s is handling a request : %v ...", c.cid, req)
	switch req.Flag2() {
	case dtype.HDP_DATA_ACK:
		logger.Debugf("%s is reading a HDP_DATA_ACK message ...", c.cid)
		c.parseHeader()
	case dtype.HDP_READ1:
		if req.Value("deadline") == nil {
			c.readFrame(req)
			return
		}
		c.setDeadline(req)
	default:
	}
}

// ===========================================================================
func (c *hdpRead2) parseHeader() (dtype.HDP_STATE2, []byte, error) {
	hdr := make([]byte, 18)
	logger.Debugf("%s parseHeader is running with refNum : %v ...", c.cid, c.refNum)
	_, err := c.read1(hdr)
	if err != nil {
		return dtype.HDP_ERROR, nil, err
	}

	refNum := binary.LittleEndian.Uint16(hdr[:2])

	if c.refNum[0] != 0 && c.refNum[0] != refNum {
		// reject the request and respond to the remotePeer
		logger.Debugf("######## %s got wrong peer refnum : %d, %d", c.cid, c.refNum[0], refNum)
		err = NewHdpError(ErrCodeWrongPeerRefNum)
		return dtype.HDP_ERROR, nil, err
	}

	err = verifyChecksum1(c.cid, hdr)
	if err != nil {
		err = NewHdpError(ErrUnequalChecksum, c.cid, "parseHeader", err)
		return dtype.HDP_ERROR, nil, err
	}

	flag := dtype.HDP_STATE2(hdr[10])
	switch flag {
	case dtype.HDP_RMT_CLOSING:
		logger.Debugf("%s got HDP_RMT_CLOSING notice", c.cid)
		return dtype.HDP_RMT_CLOSING, nil, io.EOF
	case dtype.HDP_DATA_ACK:
		logger.Debugf("%s got HDP_DATA_ACK seqNum : %d", c.cid, binary.LittleEndian.Uint32(hdr[6:10]))
		c.tpt[W] <- newHdpEvent(dtype.HDP_DATA_ACK, ":data", "bytes", hdr[2:10])
	case dtype.HDP_DATAGRAM:
		// push the peer seqNum and acknowledged local seqNum to the write channel
		// and append the data size and checksum
		logger.Debugf("%s got HDP_DATAGRAM frame with seqNum : %d", c.cid, binary.LittleEndian.Uint32(hdr[2:6]))
		c.tpt[W] <- newHdpEvent(dtype.HDP_DATAGRAM, ":data", "bytes", hdr[2:10])
	}
	return flag, hdr[10:16], nil
}

// ===========================================================================
func (c *hdpRead2) read1(b []byte) (int, error) {
	// don't need to lock here because AioListener is the exclusive reader of <l.connex.pname>.accept
	logger.Debugf("############## %s is listening : ##################", c.cid)
	err := <-waitIoReady(c.cid, c.fd, EV_READ)
	logger.Debugf("@@@@@@@@@@@@@@@@@ %s got io-ready result : %v", c.cid, err)
	if err != nil {
		return 0, err
	}
	for range 3 {
		n, err := c.read(b)
		if err != nil {
			switch err {
			case unix.EAGAIN:
				<-time.After(100 * time.Millisecond)
				continue
			}
			return 0, err
		}
		return n, nil
	}
	logger.Debugf("@@@@@@@@@@@@@@ %s exhausted EAGAIN read retries ...", c.cid)
	return 0, io.EOF
}

// ===========================================================================
func (c *hdpRead2) run() {
	logger.Debugf("%s[%d] is running ...", c.cid, c.fd)
	for ev := range c.tpt[R] {
		err := ev.Err()
		if err != nil {
			logger.Debugf("@@@@@@@@@@@ %s got an error : %v @@@@@@@@@@@@@@@@", c.cid, ev.Err())
			return
		}
		logger.Debugf("@@@@@@@@@@@ %s got a new read event : %v @@@@@@@@@@@@@@@@", c.cid, ev)
		switch ev.Flag2() {
		default:
			c.handle(ev)
		}
	}
}

// ===========================================================================
func (c *hdpRead2) readFrame(req dtype.HdpEvent) {
	logger.Debugf("$$$$$$$$$$$ %s[%d] is reading a new frame ...", c.cid, c.fd)
	if c.buffer.Len() > 0 {
		b := req.Bytes()
		logger.Debugf("%s writing buffered[%d] to user frame ...", c.cid, c.buffer.Len())
		n, err := c.buffer.Read(b)
		if err == io.EOF {
			// ignore buffer EOF error which just means empty. if the frame is partially filled
			// then the user-app can read the conn again if it expects more data is available.
			err = nil
		}
		req.Ch() <- req.With(err, ":data", "byteNum", n)
		return
	}
	flag, data, err := c.parseHeader()
	if err != nil {
		req.Ch() <- req.With(err, ":data", "byteNum", 0)
		return
	} else if flag == dtype.HDP_DATA_ACK {
		logger.Debugf("%s got HDP_DATA_ACK event from peer conn", c.cid)
		req.Ch() <- newHdpEvent()
		return
	}
	// set the original request flag which the caller depends on to resume eventloop activity
	c.read2(req, data)
}

// ===========================================================================
func (c *hdpRead2) read2(req dtype.HdpEvent, data []byte) {
	logger.Debugf("$$$$$$$$$$$ %s[%d] is reading frame2 ...", c.cid, c.fd)
	// logger.Debugf("%s is wanting read readiness ...", c.cid)
	b := req.Bytes()
	if len(b) == 0 {
		logger.Debugf("%s got 0 data size, aborting read ...", c.cid)
		return
	}
	dsize := int(binary.LittleEndian.Uint16(data[2:4]))
	n, err := c.read3(b, dsize, binary.LittleEndian.Uint16(data[4:6]))
	logger.Debugf("%s returning HDP_DATAGRAM read result : %s", c.cid, b)
	req.Ch() <- req.With(err, ":data", "byteNum", n)
}

// ===========================================================================
func (c *hdpRead2) read3(f []byte, dsize int, crc uint16) (int, error) {
	fsize := len(f)
	logger.Debugf("%s got frame and data size : %d, %d ...", c.cid, len(f), dsize)
	if fsize >= dsize {
		// read straight into the user frame
		n, err := c.read(f)
		if err != nil {
			return n, err
		}
		return n, c.verifyChecksum(f[:dsize], crc)
	}
	logger.Debugf("%s peek reading %d bytes into frame ...", c.cid, fsize)
	n, _, err := c.readFrom(f, unix.MSG_PEEK)
	if err != nil {
		return n, err
	}
	// available data size is greater than the user frame size, so buffer the remainder
	nn, err1 := c.buffer.ReadFrom(c.socket, n, dsize, crc)
	logger.Debugf("%s buffered remainder read count and error : %d, %v", c.cid, nn, err1)
	return n, err
}

// ===========================================================================
func (c *hdpRead2) setDeadline(req dtype.HdpEvent) {
	logger.Debugf("$$$$$$$$$$$ %s[%d] is setting a read deadline ...", c.cid, c.fd)
	err := <-epoller.SubmitIoReq(newHdpEvent(":data",
		"reqRef/cid", c.cid,
		"reqRef/fd", c.fd,
		"reqRef/flags", int(unix.EPOLLIN),
		"reqRef/mode", int(EV_READ),
		"reqRef/deadline", req.Value("deadline")))
	req.Ch() <- req.With(err)
}

// ===========================================================================
type hdpWrite2 struct {
	*socket
	ackTimeout time.Duration
	ackDoneCh  map[uint32]chan bool
	buffer     BufferW
	cid        string
	rb         *ringBuffer
	refNum     *[2]uint16
	// state      [2]dtype.HDP_STATE2
	tpt dtype.MultiCh
}

// ===========================================================================
func (c *hdpWrite2) handle(req dtype.HdpEvent) {
	logger.Debugf("%s is handling a request : %v ...", c.cid, req)
	switch req.Flag2() {
	case dtype.HDP_DATA_ACK:
		c.resetAckSeqnum(req)
	case dtype.HDP_DATAGRAM:
		c.acknowSeqnum(req)
	case dtype.HDP_REPEAT:
		logger.Debugf("%s got a HDP_REPEAT acknowledgement timeout resend request", c.cid)
	case dtype.HDP_WRITE1:
		if req.Value("deadline") == nil {
			c.putFrame(req)
			return
		}
		c.setDeadline(req)
	case dtype.HDP_WRITE2:
		c.writeFrame(req)
	}
}

// ===========================================================================
func (c *hdpWrite2) acknowSeqnum(req dtype.HdpEvent) {
	data := req.Bytes()
	seqNum := binary.LittleEndian.Uint32(data[:4])
	logger.Debugf("%s is writing a seqNum[%d] acknowledgement to the peer conn ...", c.cid, seqNum)
	_, _ = c.writeHeader1(0, seqNum, dtype.HDP_DATA_ACK, 0, 0)
}

// ===========================================================================
func (c *hdpWrite2) putFrame(req dtype.HdpEvent) {
	_, err := func() (int, error) {
		if c.buffer.isFull() {
			logger.Debugf("%s buffer is full : %v", c.cid, req)
			return 0, VErrEOF
		}
		logger.Debugf("%s getting next seqNum ...", c.cid)
		seqNum, err := c.rb.nextSeqNum()
		if err != nil {
			return 0, err
		}
		if !c.rb.windowFull() { // only proceed with frame write if ringBuffer is not full
			logger.Debugf("%s ringbuffer has capacity ...", c.cid)
			c.tpt[W] <- req.With(dtype.HDP_WRITE2, ":data", "seqNum", seqNum)
		}
		// if frame buffer is not full, add the next frame
		return c.buffer.addEntry1(req.Bytes(), seqNum, c.cid)
	}()
	if err != nil {
		req.Ch() <- req.With(err)
	}
}

// ===========================================================================
func (c *hdpWrite2) resetAckSeqnum(req dtype.HdpEvent) {
	logger.Debugf("%s is checking peer conn seqNum acknowledgement ...", c.cid)
	data := req.Bytes()
	seqNum := binary.LittleEndian.Uint32(data[4:])
	doneCh, _ := c.ackDoneCh[seqNum]
	if doneCh != nil {
		logger.Debugf("%s got peer conn seqNum[%d] acknowledgement ...", c.cid, seqNum)
		doneCh <- true
	}
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
		logger.Debugf("@@@@@@@@@@@ %s got a new write event : %v @@@@@@@@@@@@@@@@", c.cid, ev)
		switch ev.Flag2() {
		default:
			c.handle(ev)
		}
	}
}

// ===========================================================================
func (c *hdpWrite2) setDeadline(req dtype.HdpEvent) {
	logger.Debugf("$$$$$$$$$$$ %s[%d] is setting a write deadline ...", c.cid, c.fd)
	err := <-epoller.SubmitIoReq(newHdpEvent(":data",
		"reqRef/cid", c.cid,
		"reqRef/fd", c.fd,
		"reqRef/flags", int(unix.EPOLLOUT),
		"reqRef/mode", int(EV_WRITE),
		"reqRef/deadline", req.Value("deadline")))
	req.Ch() <- req.With(err)
}

// ===========================================================================
func (c *hdpWrite2) write1(b []byte) (int, error) {
	for range 3 {
		n, err := c.write(b)
		if err != nil {
			switch err {
			case unix.EAGAIN:
				<-time.After(100 * time.Millisecond)
				continue
			}
			return n, err
		}
		logger.Debugf("############# wrote bytes : %d", n)
		return n, nil
	}
	logger.Debugf("@@@@@@@@@@@@@@ %s exhausted EAGAIN write retries ...", c.cid)
	return 0, io.EOF
}

// ===========================================================================
func (c *hdpWrite2) writeFrame(req dtype.HdpEvent) {
	fr := req.Frame()
	logger.Debugf("$$$$$$$$$$$ %s[%d] got a new write frame : %s $$$$$$$$$$$$$", c.cid, c.fd, fr.B)
	err := <-waitIoReady(c.cid, c.fd, EV_WRITE)
	logger.Debugf("@@@@@@@@@@@@@@@@@ %s got io-ready result : %v", c.cid, err)
	if err != nil {
		req.Ch() <- req.With(err, ":data", "byteNum", 0)
		return
	}
	seqNum := req.UInt32("seqNum")
	logger.Debugf("@@@@@@@@@@@@@@@@@ %s got seqNum for write header : %d", c.cid, seqNum)
	_, err = c.writeHeader(seqNum, 0, fr.B)
	if err != nil {
		req.Ch() <- newHdpEvent(err)
		return
	}
	fr.N, err = c.write1(fr.B)
	if err != nil {
		req.Ch() <- newHdpEvent(err)
		return
	}
	ch := make(chan bool, 1)
	c.rb.setNextItem(seqNum)
	c.ackDoneCh[seqNum] = ch
	go func() {
		select {
		case <-time.After(c.ackTimeout):
			c.tpt[W] <- req.With(dtype.HDP_REPEAT)
		case <-ch:
			logger.Debugf("%s frame[%d] was acknowledged by the peer conn", c.cid, seqNum)
			c.rb.updateAckSeqNum(seqNum)
			return
		}
	}()
	// notify reader to read the expected HDP_DATA_ACK message
	c.tpt[R] <- newHdpEvent(dtype.HDP_DATA_ACK)
	logger.Debugf("%s returning HDP_WRITE1 write result : %d", c.cid, fr.N)
	req.Ch() <- req.With(err, ":data", "byteNum", fr.N)
}

// ===========================================================================
func (c *hdpWrite2) writeHeader(seqNum uint32, ackSeqNum uint32, frame []byte) (int, error) {
	crc := crc16.Checksum(frame, crc16.IBMTable)
	return c.writeHeader1(seqNum, ackSeqNum, dtype.HDP_DATAGRAM, len(frame), crc)
}

// ===========================================================================
func (c *hdpWrite2) writeHeader1(seqNum uint32, ackSeqNum uint32, flag dtype.HDP_STATE2, fsize int, crc uint16) (int, error) {
	hdr := make([]byte, 18)
	logger.Debugf("%s writing refNum[1] in header : %v", c.cid, c.refNum[1])
	binary.LittleEndian.PutUint16(hdr[0:2], c.refNum[1])
	binary.LittleEndian.PutUint32(hdr[2:6], seqNum)
	binary.LittleEndian.PutUint32(hdr[6:10], ackSeqNum)
	// set the FLG_ACK flags
	hdr[10] = byte(flag) // FLG_ACK
	// insert the header checksum
	binary.LittleEndian.PutUint16(hdr[12:14], uint16(fsize))
	binary.LittleEndian.PutUint16(hdr[14:16], crc)
	binary.LittleEndian.PutUint16(hdr[16:], crc16.Checksum(hdr, crc16.IBMTable))
	return c.write1(hdr)
}
