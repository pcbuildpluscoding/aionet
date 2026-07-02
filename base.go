package aionet

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/howeyc/crc16"
	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/logroll"
)

var (
	logger   *logroll.LogFile
	zeroTime = time.Time{}
)

// ===========================================================================
func init() {
	logger = logroll.New()
}

// ===========================================================================
func SetLogger(super *logroll.LogFile) {
	logger = super
}

// ===========================================================================
func newHdpEvent(args ...any) dtype.HdpEvent {
	x := dtype.HdpEvent{}
	return x.With(args...)
}

// ===========================================================================
func verifyChecksum1(cid string, b []byte) error {
	crc := binary.LittleEndian.Uint16(b[12:14])
	b[12] = 0
	b[13] = 0
	// crc1 := crc16.Checksum(b, crc16.IBMTable)
	// logger.Debugf("%s got frame size[%d] and checksum, and calculated : %d, %d", cid, len(b), crc, crc1)
	if crc != crc16.Checksum(b, crc16.IBMTable) {
		return fmt.Errorf("%s checksum verification failed", cid) // syscall.ECONNABORTED
	}
	return nil
}
