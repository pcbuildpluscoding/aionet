package aionet

import (
	"fmt"
	"math"
)

func newRingItem1(seqNum uint32) *ringItem1 {
	return &ringItem1{
		seqNum: seqNum,
	}
}

// ==================================================================//
// gyroNumbr
// ==================================================================//
type gyroNumbr struct {
	seqNum      uint32
	oldestUnack *ringItem1
	window      uint32
}

// ---------------------------------------------------------------//
// windowFull
// ---------------------------------------------------------------//
func (g *gyroNumbr) windowFull() bool {
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
func (g *gyroNumbr) init() {
	g.oldestUnack = nil
	g.seqNum = 1
}

// ---------------------------------------------------------------//
// modulus
// ---------------------------------------------------------------//
func (g *gyroNumbr) modulus(seqNum uint32) uint32 {
	return (seqNum - 1) % g.window
}

// ---------------------------------------------------------------//
// next
// ---------------------------------------------------------------//
func (g *gyroNumbr) next() (uint32, uint32, error) {
	if g.seqNum == math.MaxUint32 {
		return 0, 0, NewHdpError(ErrSeqNumOverflow)
	}
	i := g.modulus(g.seqNum)
	seqNum := g.seqNum
	if g.seqNum+1 == math.MaxUint32 {
		g.seqNum = 1
		// g.recycled = true
	} else {
		g.seqNum++
	}
	return i, seqNum, nil
}

// ---------------------------------------------------------------//
// HasCapacity
// ---------------------------------------------------------------//
func (g *gyroNumbr) updateUnAck(item *ringItem1) {
	// if item != nil && item.acknowledged {
	// 	logger.Debugf("unacknowledged seqNum is nil")
	// 	g.oldestUnack = nil
	// 	return
	// }
	g.oldestUnack = item
}

// ---------------------------------------------------------------//
// ackNumIsOldest
// ---------------------------------------------------------------//
func (g *gyroNumbr) ackNumIsOldest(item *ringItem1) bool {
	if g.oldestUnack == nil {
		return true
	}
	return g.oldestUnack == item
}

// ==================================================================//
// ringItem
// ==================================================================//
type ringItem1 struct {
	seqNum       uint32
	acknowledged bool
}

// ---------------------------------------------------------------//
// reset
// ---------------------------------------------------------------//
func (i *ringItem1) reset() {
	i.seqNum = 0
	i.acknowledged = true
}

// ==================================================================//
// ringBuffr
// ==================================================================//
type ringBuffr struct {
	gyro gyroNumbr
	this []*ringItem1
}

// ==================================================================
func (b *ringBuffr) windowFull() bool {
	return b.gyro.windowFull()
}

// ==================================================================
func (b *ringBuffr) nextSeqNum() (uint32, error) {
	if b.gyro.windowFull() {
		return 0, NewHdpError(ErrRingBufferFull)
	}
	_, seqNum, err := b.gyro.next()
	return seqNum, err
}

// ==================================================================
func (b *ringBuffr) setNextItem(seqNum uint32) {
	i := b.gyro.modulus(seqNum)
	item := newRingItem1(seqNum)
	if b.gyro.oldestUnack == nil {
		b.gyro.oldestUnack = item
	}
	b.this[i] = item
	// logger.Debugf("$$$$$$ next send rindex, seqNum : %d, %d", i, seqNum)
}

// ==================================================================
func (b *ringBuffr) updateAckSeqNum(seqNum uint32) error {
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
