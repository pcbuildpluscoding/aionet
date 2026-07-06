package aionet

import (
	"fmt"
	"math"
)

func newRingItem(seqNum uint32) *ringItem {
	return &ringItem{
		seqNum: seqNum,
	}
}

// ==================================================================//
// gyroNumber
// ==================================================================//
type gyroNumber struct {
	recycled   bool
	seqNum     uint32
	firstUnAck *ringItem
	window     uint32
}

// ---------------------------------------------------------------//
// windowIsFull
// ---------------------------------------------------------------//
func (g *gyroNumber) windowIsFull() bool {
	if g.firstUnAck == nil {
		return false
	}
	diff := g.seqNum - g.firstUnAck.seqNum
	mod := diff % g.window
	// logger.Debugf("full testing, seqNum, firstUnAck, full? : %d, %d, %v", g.seqNum, g.firstUnAck.seqNum, g.seqNum > 0 && mod == 0)
	if g.recycled {
		return mod == 0
	}
	return g.seqNum > 1 && mod == 0
}

// ---------------------------------------------------------------//
// init1
// ---------------------------------------------------------------//
func (g *gyroNumber) init() {
	g.firstUnAck = nil
	g.seqNum = 1
}

// ---------------------------------------------------------------//
// modulus
// ---------------------------------------------------------------//
func (g *gyroNumber) modulus(seqNum uint32) uint32 {
	return (seqNum - 1) % g.window
}

// ---------------------------------------------------------------//
// next
// ---------------------------------------------------------------//
func (g *gyroNumber) next() (uint32, uint32, error) {
	if g.seqNum == math.MaxUint32 {
		return 0, 0, NewHdpError(ErrSeqNumOverflow)
	}
	i := g.modulus(g.seqNum)
	seqNum := g.seqNum
	if g.seqNum+1 == math.MaxUint32 {
		g.seqNum = 1
		g.recycled = true
	} else {
		g.seqNum++
	}
	return i, seqNum, nil
}

// ---------------------------------------------------------------//
// HasCapacity
// ---------------------------------------------------------------//
func (g *gyroNumber) updateUnAck(item *ringItem) {
	if item != nil && item.sentAck {
		// logger.Debugf("unacknowledged seqNum is nil")
		g.firstUnAck = nil
		return
	}
	g.firstUnAck = item
}

// ---------------------------------------------------------------//
// ackNumIsOldest
// ---------------------------------------------------------------//
func (g *gyroNumber) ackNumIsOldest(item *ringItem) bool {
	if g.firstUnAck == nil {
		return true
	}
	return g.firstUnAck == item
}

// ==================================================================//
// ringItem
// ==================================================================//
type ringItem struct {
	seqNum  uint32
	sentAck bool
}

// ---------------------------------------------------------------//
// reset
// ---------------------------------------------------------------//
func (i *ringItem) reset() {
	i.seqNum = 0
	i.sentAck = true
}

// ==================================================================//
// ringBuffer
// ==================================================================//
type ringBuffer struct {
	gyro gyroNumber
	this []*ringItem
}

// ==================================================================
func (b *ringBuffer) hasCapacity() bool {
	return !b.gyro.windowIsFull()
}

// ==================================================================
func (b *ringBuffer) nextSeqNum() (uint32, error) {
	// equals 1 for the second item added
	// if b.gyro.windowIsFull() {
	// 	return 0, NewHdpError(ErrRingBufferFull)
	// }
	i, seqNum, err := b.gyro.next()
	if err == nil {
		item := newRingItem(seqNum)
		if b.gyro.firstUnAck == nil {
			b.gyro.firstUnAck = item
		}
		b.this[i] = item
	}
	// logger.Debugf("$$$$$$ next send rindex, seqNum : %d, %d", i, seqNum)
	return seqNum, err
}

// ==================================================================
func (b *ringBuffer) updateAckSeqNum(seqNum uint32) error {
	// sanity check
	j := b.gyro.modulus(seqNum)
	item := b.this[j]
	if item == nil {
		return fmt.Errorf("%d seqNumAck is invalid", seqNum)
	} else if item.seqNum != seqNum {
		if item.seqNum == 0 && item.sentAck {
			// logger.Debugf("seqNum %d is already acknowledged", seqNum)
			return nil
		}
		return fmt.Errorf("seqNumAck does not match the reference value : %d, %d", seqNum, item.seqNum)
	}
	// logger.Debugf("acknowledged index, seqNum, ringItem.seqNum : %d, %d, %d", j, seqNum, item.seqNum)
	if !b.gyro.ackNumIsOldest(b.this[j]) {
		item.reset()
		// logger.Debugf("acknowSeqNum %d is not the oldest for this session : %d", seqNum, b.gyro.firstUnAck.seqNum)
		return nil
	}
	item.reset()
	k := uint32(0)
	// range over the list from starting at j+1, ending back at j if an unAckSeqNum is not found.
	for i := uint32(1); i <= b.gyro.window; i++ {
		k = (j + i) % b.gyro.window
		item = b.this[k]
		if item != nil && !item.sentAck {
			break
		}
	}
	if item != nil {
		// logger.Debugf("next unacknowledged index, seqNum : %d, %d", k, b.this[k].seqNum)
	}
	b.gyro.updateUnAck(b.this[k])
	return nil
}
