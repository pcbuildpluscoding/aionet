package aionet

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/howeyc/crc16"
	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/aionet/epoller"
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
func newBufEntry(b []byte, key uint32) *bufEntry {
	return &bufEntry{
		frame:    [2][]byte{0: b, 1: nil},
		ackCh:    make(chan bool),
		timerKey: key,
	}
}

// ===========================================================================
func newBufEntry1(b []byte) *bufEntry {
	return &bufEntry{
		frame: [2][]byte{0: b, 1: nil},
	}
}

// ===========================================================================
func newBufferW(writeBufferSize int) BufferW {
	if writeBufferSize < 128 {
		writeBufferSize = 128
	}
	return BufferW{
		this:   map[uint32]*bufEntry{},
		resend: []uint32{},
		seqNum: []uint32{},
		size:   [2]int{0, writeBufferSize},
	}
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
func newHdpRead1(s socket1, rn *[2]uint16, tpt dtype.MultiCh) *hdpRead1 {
	return &hdpRead1{
		socket1: s,
		refNum:  rn,
		tpt:     tpt,
	}
}

// ===========================================================================
func newHdpWrite1(s socket1, rn *[2]uint16, tpt dtype.MultiCh) *hdpWrite1 {
	return &hdpWrite1{
		socket1: s,
		refNum:  rn,
		tpt:     tpt,
	}
}

// ===========================================================================
func newHdpRead2(s *socket, cid string, rn *[2]uint16, tpt dtype.MultiCh) *hdpRead2 {
	return &hdpRead2{
		socket: s,
		cid:    cid,
		refNum: rn,
		tpt:    tpt,
	}
}

// ===========================================================================
func newHdpWrite2(s *socket, cid string, rn *[2]uint16, tpt dtype.MultiCh, windowSize uint16) *hdpWrite2 {
	return &hdpWrite2{
		socket:     s,
		ackTimeout: 10 * time.Second,
		cid:        cid,
		buffer:     newBufferW(32),
		rb:         newRingBuffer(uint32(windowSize)),
		refNum:     rn,
		tpt:        tpt,
	}
}

// ===========================================================================
func newRingBuffer(windowSize uint32) *ringBuffer {
	gyro := gyroNumber{
		window: windowSize,
	}
	rb := &ringBuffer{
		gyro: gyro,
		this: make([]*ringItem, windowSize),
	}
	rb.gyro.init()
	return rb
}

// func newWriter3(s1 *socket1, rn *[2]uint16, req ioResult) *hdpWrite3 {
// 	writeAckTimeout := 1000
// 	timeout := req.Int("writeAckTimeout")
// 	if timeout > 499 {
// 		writeAckTimeout = timeout
// 	}
// 	bufferSize := 1024
// 	bsize := req.Int("writeBufferSize")
// 	if bsize > 31 {
// 		bufferSize = bsize
// 	}
// 	if !req.HasKeys("eventCh") {
// 		panic(fmt.Errorf("%s hdpWrite3 parameter chan writeReq is required", s1.cid))
// 	}
// 	eventCh, ok := req.Value("eventCh").([2]eventTpt)
// 	if !ok {
// 		panic("hdpWrite5 requires eventCh type == [2]eventTpt")
// 	}
// 	return &hdpWrite3{
// 		ackTimeout: time.Duration(writeAckTimeout) * time.Millisecond,
// 		socket5:    s1.newSocket5(eventCh),
// 		refNum:     rn,
// 		rb:         NewRingBuffer(16, uint32(s1.windowSize)),
// 		buffer:     newBufferW3(bufferSize),
// 		tw:         req.Value("testware").(*AdaptorTest),
// 	}
// }

// ===========================================================================
func NewHdpDialer() (*HdpDialer, error) {
	cid := "hdpDialer-" + time.Now().Format("05.00000")
	d := &HdpDialer{
		cid: cid,
		tpt: dtype.MultiCh{make(chan dtype.HdpEvent, 1), make(chan dtype.HdpEvent, 1)},
	}
	return d.start()
}

// ===========================================================================
func NewHdpListener0(network, address string) (*HdpListener0, error) {
	laddr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	logger.Debugf("newHDPListener is calling newSocket ...")
	s, err := newSocket(laddr, nil)
	if err != nil {
		return nil, err
	}
	l := &HdpListener0{
		socket:         s,
		cid:            "hdpListener-" + time.Now().Format("05.00000"),
		connectTimeout: time.Duration(30) * time.Second,
		tpt:            dtype.MultiCh{make(chan dtype.HdpEvent, 1), make(chan dtype.HdpEvent, 1)},
	}
	return l.start1()
}

// ===========================================================================
func NewHdpListener(network, address string, windowSize uint16) (*HdpListener, error) {
	laddr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	logger.Debugf("newHDPListener is calling newSocket ...")
	s, err := newSocket(laddr, nil)
	if err != nil {
		return nil, err
	}
	cid := "hdpListener-" + time.Now().Format("05.00000")
	l := &HdpListener{
		socket1:        s.newSocket1(cid, windowSize),
		connectTimeout: time.Duration(30) * time.Second,
		tpt:            dtype.MultiCh{make(chan dtype.HdpEvent, 1), make(chan dtype.HdpEvent, 1)},
	}
	return l.start()
}

// ===========================================================================
func newSocket0(mode string, sotype, proto int, laddr, raddr *net.UDPAddr) (*socket, error) {
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
func newSocket(laddr, raddr *net.UDPAddr) (*socket, error) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}
	return &socket{
		fd:     fd,
		family: unix.AF_INET,
		laddr:  laddr,
		raddr:  raddr,
	}, nil
}

// ===========================================================================
func verifyChecksum1(cid string, b []byte) error {
	crc := binary.LittleEndian.Uint16(b[16:18])
	b[16] = 0
	b[17] = 0
	// crc1 := crc16.Checksum(b, crc16.IBMTable)
	// logger.Debugf("%s got frame size[%d] and checksum, and calculated : %d, %d", cid, len(b), crc, crc1)
	if crc != crc16.Checksum(b, crc16.IBMTable) {
		return fmt.Errorf("%s checksum verification failed", cid) // unix.ECONNABORTED
	}
	return nil
}

// ===========================================================================
func waitIoReady(cid string, fd int, mode ioMode) chan error {
	var flag int
	switch mode {
	case EV_READ:
		flag = unix.EPOLLIN
	case EV_WRITE:
		flag = unix.EPOLLOUT
	}
	ev := newHdpEvent(":data",
		"ioReq/op", unix.EPOLL_CTL_MOD,
		"reqRef/cid", cid,
		"reqRef/fd", fd,
		"reqRef/flags", flag,
		"reqRef/mode", int(mode))
	return epoller.SubmitIoReq(ev)
}
