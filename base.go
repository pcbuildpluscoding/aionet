package aionet

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/howeyc/crc16"
	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/logroll"
	"golang.org/x/sys/unix"
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
func getRefNum() uint16 {
	id := uint16(time.Now().Nanosecond() % 65536)
	if id >= 1000 {
		return id
	}
	return 1000 + id
}

// ===========================================================================
func newHdpRead1(s *socket1, rn *[2]uint16, tpt dtype.MultiCh) *hdpRead1 {
	return &hdpRead1{
		socket1: s,
		refNum:  rn,
		tpt:     tpt,
	}
}

// ===========================================================================
func newHdpWrite1(s *socket1, rn *[2]uint16, tpt dtype.MultiCh) *hdpWrite1 {
	return &hdpWrite1{
		socket1: s,
		refNum:  rn,
		tpt:     tpt,
	}
}

// ===========================================================================
func newHdpRead2(s *socket, rn *[2]uint16, tpt dtype.MultiCh) *hdpRead2 {
	return &hdpRead2{
		socket: s,
		refNum: rn,
		tpt:    tpt,
	}
}

// ===========================================================================
func newHdpWrite2(s *socket, rn *[2]uint16) *hdpWrite2 {
	return &hdpWrite2{
		socket: s,
		refNum: rn,
	}
}

// ===========================================================================
func NewHdpDialer() *HdpDialer {
	cid := "hdpDialer-%s" + time.Now().Format("05.00000")
	return &HdpDialer{
		cid: cid,
		tpt: dtype.MultiCh{},
	}
}

// ===========================================================================
func NewHdpListener(network, address string) (*HdpListener, error) {
	laddr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	logger.Debugf("newHDPListener is calling newSocket ...")
	s, err := newSocket("listen", unix.SOCK_DGRAM, 0, laddr, nil)
	if err != nil {
		return nil, err
	}
	s.cid = "hdpListener-%s" + time.Now().Format("05.00000")
	l := &HdpListener{
		socket:         s,
		connectTimeout: time.Duration(30) * time.Second,
		tpt:            dtype.MultiCh{},
	}
	return l.start(), s.init()
}

// ===========================================================================
func newSocket(mode string, sotype, proto int, laddr, raddr *net.UDPAddr) (*socket, error) {
	laddr1 := &sockAddr{Addr: laddr}
	family, ipv6only := favoriteAddrFamily(laddr1.Network(), laddr1, nil, mode)
	fd, err := unix.Socket(family, sotype|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, proto)
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}
	return &socket{
		fd:     fd,
		family: family,
		laddr:  laddr,
		raddr:  raddr,
	}, setDefaultSockopts(fd, family, sotype, ipv6only)
}

// ===========================================================================
func verifyChecksum1(cid string, b []byte) error {
	crc := binary.LittleEndian.Uint16(b[12:14])
	b[12] = 0
	b[13] = 0
	// crc1 := crc16.Checksum(b, crc16.IBMTable)
	// logger.Debugf("%s got frame size[%d] and checksum, and calculated : %d, %d", cid, len(b), crc, crc1)
	if crc != crc16.Checksum(b, crc16.IBMTable) {
		return fmt.Errorf("%s checksum verification failed", cid) // unix.ECONNABORTED
	}
	return nil
}
