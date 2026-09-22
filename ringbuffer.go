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
	seqNum      uint32
	oldestUnack *ringItem
	window      uint32
}

// ---------------------------------------------------------------//
// windowFull
// ---------------------------------------------------------------//
func (g *gyroNumber) windowFull() bool {
	if g.oldestUnack == nil {
		return false
	}
	diff := g.seqNum - g.oldestUnack.seqNum
	mod := diff % g.window
	// logger.Debugf("full testing, seqNum, oldestUnack, full? : %d, %d, %v", g.seqNum, g.oldestUnack.seqNum, g.seqNum > 0 && mod == 0)
	return mod == 0
}

// ---------------------------------------------------------------//
// init1
// ---------------------------------------------------------------//
func (g *gyroNumber) init() {
	g.oldestUnack = nil
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
	} else {
		g.seqNum++
	}
	return i, seqNum, nil
}

// ---------------------------------------------------------------//
// HasCapacity
// ---------------------------------------------------------------//
func (g *gyroNumber) updateUnAck(item *ringItem) {
	g.oldestUnack = item
}

// ---------------------------------------------------------------//
// ackNumIsOldest
// ---------------------------------------------------------------//
func (g *gyroNumber) ackNumIsOldest(item *ringItem) bool {
	if g.oldestUnack == nil {
		return true
	}
	return g.oldestUnack == item
}

// ==================================================================//
// ringItem
// ==================================================================//
type ringItem struct {
	seqNum       uint32
	acknowledged bool
}

// ---------------------------------------------------------------//
// reset
// ---------------------------------------------------------------//
func (i *ringItem) reset() {
	i.seqNum = 0
	i.acknowledged = true
}

// ==================================================================//
// ringBuffer
// ==================================================================//
type ringBuffer struct {
	gyro gyroNumber
	this []*ringItem
}

// ==================================================================
func (b *ringBuffer) windowFull() bool {
	return b.gyro.windowFull()
}

// ==================================================================
func (b *ringBuffer) nextSeqNum() (uint32, error) {
	if b.gyro.windowFull() {
		return 0, NewHdpError(ErrRingBufferFull)
	}
	_, seqNum, err := b.gyro.next()
	return seqNum, err
}

// ==================================================================
func (b *ringBuffer) setNextItem(seqNum uint32) {
	// if b.gyro.windowFull() {
	// 	return 0, NewHdpError(ErrRingBufferFull)
	// }
	i := b.gyro.modulus(seqNum)
	item := newRingItem(seqNum)
	if b.gyro.oldestUnack == nil {
		b.gyro.oldestUnack = item
	}
	b.this[i] = item
	// logger.Debugf("$$$$$$ next send rindex, seqNum : %d, %d", i, seqNum)
}

// ==================================================================
func (b *ringBuffer) updateAckSeqNum(seqNum uint32) error {
	// sanity check
	j := b.gyro.modulus(seqNum)
	item := b.this[j]
	if item == nil {
		return fmt.Errorf("%d seqNumAck is invalid", seqNum)
	} else if item.seqNum != seqNum {
		if item.seqNum == 0 && item.acknowledged {
			logger.Debugf("seqNum %d is already acknowledged", seqNum)
			return nil
		}
		return fmt.Errorf("seqNumAck does not match the reference value : %d, %d", seqNum, item.seqNum)
	}
	logger.Debugf("acknowledged index, seqNum, ringItem.seqNum : %d, %d, %d", j, seqNum, item.seqNum)
	if !b.gyro.ackNumIsOldest(b.this[j]) {
		item.reset()
		logger.Debugf("acknowSeqNum %d is not the oldest for this session : %d", seqNum, b.gyro.oldestUnack.seqNum)
		return nil
	}
	item.reset()
	k := uint32(0)
	// range over the list from starting at j+1, ending back at j if an unAckSeqNum is not found.
	for i := uint32(1); i <= b.gyro.window; i++ {
		k = (j + i) % b.gyro.window
		// logger.Debugf("next window index : %d", k)
		item = b.this[k]
		if item != nil && !item.acknowledged {
			b.gyro.oldestUnack = item
			logger.Debugf("next unacknowledged seqnum index, seqNum : %d, %d", k, b.this[k].seqNum)
			return nil
		}
	}
	logger.Debugf("ringbuffer has no unacknowledged items")
	return nil
}
