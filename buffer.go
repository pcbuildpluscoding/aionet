package aionet

import (
	"bytes"
	"fmt"
	"io"
	"syscall"
)

// ================================================================//
// bufEntry
// ================================================================//
type bufEntry struct {
	frame    [2][]byte
	ackCh    chan bool
	timerKey uint32
}

// ================================================================
func (e *bufEntry) getFrame(resend bool) []byte {
	if resend && e.frame[1] != nil && len(e.frame[1]) > 0 {
		return e.frame[1]
	}
	return e.frame[0]
}

// ================================================================//
// BufferSN
// ================================================================//
type BufferSN struct {
	seqNum []uint32
}

// ================================================================
func (b BufferSN) add(seqNum uint32) {
	b.seqNum = append(b.seqNum, seqNum)
}

// ================================================================
func (b BufferSN) isEmpty() bool {
	return len(b.seqNum) == 0
}

// ================================================================
func (b BufferSN) popLeft() uint32 {
	if len(b.seqNum) == 0 {
		return 0
	}
	seqNum := b.seqNum[0]
	b.seqNum = b.seqNum[1:]
	return seqNum
}

// ================================================================//
// BufferW
// ================================================================//
type BuffrW struct {
	this    map[uint32][]byte
	maxsize int
}

// ================================================================
func (w *BuffrW) isEmpty() bool {
	logger.Debugf("BufferW current size : %d", len(w.this))
	return len(w.this) == 0
}

// ================================================================
func (w *BuffrW) isFull() bool {
	logger.Debugf("BufferW current size : %d", len(w.this))
	return len(w.this) == w.maxsize
}

// ================================================================
func (w *BuffrW) addEntry(frame []byte, seqNum uint32, cid string) (int, error) {
	n := len(frame)
	logger.Debugf("%s BufferW seqNum[%d] for data frame[%d]", cid, seqNum, n)
	w.this[seqNum] = frame
	return n, nil
}

// ================================================================
func (b *BuffrW) pop(seqNum uint32) []byte {
	e := b.this[seqNum]
	delete(b.this, seqNum)
	return e
}

// ================================================================//
// BufferW
// ================================================================//
type BufferW struct {
	this   map[uint32]*bufEntry
	resend []uint32
	seqNum []uint32
	size   [2]int
}

// ================================================================
func (w *BufferW) addEntry(frame []byte, seqNum, timerKey uint32, cid string) (int, error) {
	n := len(frame)
	space := w.size[1] - w.size[0]
	if n > space {
		n = space
	}
	w.seqNum = append(w.seqNum, seqNum)
	logger.Debugf("%s BufferW seqNum[%d] data frame[%d, %d] and timerKey[%d] is buffered", cid, seqNum, n, len(frame), timerKey)
	w.this[seqNum] = newBufEntry(frame[:n], timerKey)
	w.size[0] += n
	return n, nil
}

// ================================================================
func (w *BufferW) addEntry1(frame []byte, seqNum uint32, cid string) (int, error) {
	n := len(frame)
	space := w.size[1] - w.size[0]
	w.seqNum = append(w.seqNum, seqNum)
	logger.Debugf("%s BufferW seqNum[%d] for data frame[%d] in space[%d]", cid, seqNum, n, space)
	w.this[seqNum] = newBufEntry1(frame)
	w.size[0] += n
	return n, nil
}

// ================================================================
func (w *BufferW) addResend(seqNum uint32) {
	w.resend = append(w.resend, seqNum)
}

// ================================================================
func (b *BufferW) getEntry(seqNum uint32) *bufEntry {
	return b.this[seqNum]
}

// ================================================================
func (w *BufferW) isEmpty() bool {
	logger.Debugf("BufferW current size : %d, %d, %d", w.size[0], w.size[1], len(w.seqNum))
	return len(w.seqNum) == 0
}

// ================================================================
func (w *BufferW) resendIsEmpty() bool {
	logger.Debugf("BufferW resend size : %d", len(w.resend))
	return len(w.resend) == 0
}

// ================================================================
func (w *BufferW) isFull() bool {
	logger.Debugf("BufferW current size : %d, %d", w.size[0], w.size[1])
	return w.size[1] <= w.size[0]
}

// ================================================================
func (b *BufferW) nextEntry(resend bool) (uint32, *bufEntry) {
	var seqNum uint32
	switch {
	case resend:
		if len(b.resend) == 0 {
			return 0, nil
		}
		seqNum = b.resend[0]
	default:
		if len(b.seqNum) == 0 {
			return 0, nil
		}
		seqNum = b.seqNum[0]
	}
	return seqNum, b.this[seqNum]
}

// ================================================================
func (b *BufferW) pop(seqNum uint32) *bufEntry {
	e := b.this[seqNum]
	delete(b.this, seqNum)
	return e
}

// ================================================================
func (b *BufferW) shiftLeft(seqNum uint32, resend bool) {
	if resend {
		if len(b.resend) == 0 {
			return
		}
		b.resend = b.resend[1:]
		return
	} else if len(b.seqNum) == 0 {
		panic(fmt.Errorf("BufferW ordered seqNum list[0] should not be empty"))
	} else if b.seqNum[0] != seqNum {
		panic(fmt.Errorf("BufferW ordered seqNum list[0] != %d, got %d instead", seqNum, b.seqNum[0]))
	}
	b.seqNum = b.seqNum[1:]
}

// ================================================================
func (b *BufferW) onFrameWrite(wn int) error {
	seqNum, e := b.nextEntry(false)
	if e == nil {
		return fmt.Errorf("BufferW system error : ordered seqNum list is empty which is not expected")
	}
	fn := len(e.frame[0])
	if wn < fn {
		logger.Debugf("BufferW for seqNum[%d] got a partial[%d] frame[%d] write result, rewriting the frame buffer ...", seqNum, wn, fn)
		e.frame[0] = e.frame[0][wn:]
	} else {
		logger.Debugf("BufferW is left shifting seqNum[%d] off the ordered seqNum list ...", seqNum)
		b.seqNum = b.seqNum[1:]
	}
	b.size[0] -= wn
	return nil
}

// ================================================================
func (b *BufferW) onFrameRewrite(wn int) error {
	seqNum, e := b.nextEntry(true)
	if e == nil {
		return fmt.Errorf("BufferW system error : ordered seqNum list is empty which is not expected")
	}
	fn := len(e.frame[0])
	if len(e.frame[1]) > 0 {
		fn = len(e.frame[1])
		if wn < fn {
			logger.Debugf("BufferW for seqNum[%d] got a partial[%d] frame[%d] resend result, rewriting the entry.resend buffer ...", seqNum, wn, fn)
			e.frame[1] = e.frame[1][wn:]
		}
		return nil
	}
	if wn < fn {
		logger.Debugf("BufferW for seqNum[%d] got a partial[%d] frame[%d] resend result, rewriting the entry.resend buffer ...", seqNum, wn, fn)
		e.frame[1] = e.frame[0][wn:]
	} else {
		logger.Debugf("BufferW is unshifting seqNum[%d] off the ordered entry.resend list ...", seqNum)
		b.resend = b.resend[1:]
	}
	return nil
}

// ================================================================//
// BufferR
// ================================================================//
type BufferR struct {
	this [][]byte
	size [2]int
}

// ================================================================
func (b *BufferR) Add(f []byte) {
	b.this = append(b.this, f)
	b.size[0] += len(f)
}

// ================================================================
func (b *BufferR) Bytes(delimiter ...[]byte) []byte {
	del := []byte("")
	if delimiter != nil {
		del = delimiter[0]
	}
	return bytes.Join(b.this, del)
}

// ================================================================
func (b *BufferR) Len() int {
	b.size[1] = 0
	for _, frame := range b.this {
		b.size[1] += len(frame)
	}
	return b.size[1]
}

// ================================================================
func (b *BufferR) isEmpty() bool {
	return b.Len() == 0
}

// ================================================================
func (b *BufferR) isFull() bool {
	b.Len()
	return b.size[1] >= b.size[0]
}

// ================================================================
// this complements bufferR.writeTo procedure
func (b *BufferR) Read(frame []byte) (int, error) {
	nn := 0
	for nn < len(frame) {
		n, err := b.writeTo(frame[nn:])
		if n > 0 {
			nn += n
		}
		logger.Debugf("@@@@@@@@@@@@@@@@@ bufferR wrote[%d] to frame ...", n)
		if err != nil {
			return nn, err
		}
	}
	return nn, nil
}

// ================================================================
func (b *BufferR) read1(c *socket, frame []byte) error {
	count := 1
	for nn := 0; nn < len(frame); {
		logger.Debugf("BufferR.read1 is reading %d bytes ...", len(frame))
		n, err := c.read(frame[nn:])
		if n > 0 {
			nn += n
		}
		if err == syscall.EAGAIN && count > 0 {
			count--
			continue
		} else if err != nil {
			logger.Errorf("BufferR got read error : %v", err)
			return err
		}
	}
	return nil
}

// ================================================================
func (b *BufferR) ReadFrom(c *socket, offset, dsize int, crc uint16) (int, error) {
	frame := make([]byte, dsize)
	err := b.read1(c, frame)
	if err != nil {
		logger.Debugf("########## Buffer got an conn.ReadFrom error : %v", err)
		return 0, err
	}
	logger.Debugf("@@@@@@@@@@ bufferR is adding a frame : %s", frame)
	b.Add(frame[offset:])
	err = c.verifyChecksum(frame, crc)
	if err != nil {
		return 0, err
	}
	logger.Debugf("@@@@@@@@@@ bufferR has resized a frame : %s", frame[offset:])
	// b.resize(frame[offset:])
	return dsize, err
}

// ================================================================
func (b *BufferR) resize(frame []byte) {
	last := len(b.this) - 1
	flast := b.this[last]
	size := len(flast) + len(frame)
	if size > b.size[1] {
		frame = append(flast, frame...)
		b.this = append(b.this[:last], frame[:b.size[1]], frame[b.size[1]:])
	} else {
		b.this[last] = append(flast, frame...)
	}
}

// ================================================================
// this complements bufferR.Read procedure
func (b *BufferR) writeTo(frame []byte) (int, error) {
	if len(b.this) == 0 {
		return 0, io.EOF
	}
	last0 := len(b.this[0]) - 1
	i := 0
	for i = range frame {
		frame[i] = b.this[0][i]
		if i == last0 {
			// len(frame) >= b.this[0]
			if len(b.this) > 0 {
				b.this = b.this[1:]
			}
			return i + 1, nil
		}
	}
	b.this[0] = b.this[0][i:]
	return i + 1, nil
}
