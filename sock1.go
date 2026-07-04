package aionet

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/howeyc/crc16"
	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/aionet/epoller"
	"golang.org/x/sys/unix"
)

// ================================================================//
// socket
// ================================================================//
type socket struct {
	cid    string
	fd     int
	family int
	laddr  net.Addr
	raddr  net.Addr
}

// ================================================================
func (c *socket) init() error {
	flags := unix.EPOLLONESHOT | unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLET
	return <-c.submitReq(unix.EPOLL_CTL_ADD, flags, EV_NULL)
}

// ================================================================
func (c *socket) submitReq(op int, flags int, mode ioMode) chan error {
	return epoller.SubmitIoReq(newHdpEvent(":data",
		"reqRef/cid", c.cid,
		"reqRef/fd", c.fd,
		"reqRef/flags", flags,
		"reqRef/mode", int(mode)))
}

// ================================================================
func (c *socket) close() error {
	var err error
	select {
	case <-time.After(time.Duration(2) * time.Second):
		err = NewHdpError(ErrTimeout, c.cid)
	case err = <-c.submitReq(unix.EPOLL_CTL_DEL, 0, EV_NULL):
		if err != nil {
			return NewHdpError(ErrOnClose, c.cid, err)
		}
	}
	// c.state = Closed
	return err
}

// ================================================================
// read - is actually a one-shot read, the loop is just for handling
// the appearance of unix.EINTR error
// ================================================================
func (c *socket) read(p []byte) (int, error) {
	nn := 0
	for {
		n, err := unix.Read(c.fd, p)
		if n > 0 {
			nn += n
		} else if err == unix.EINTR {
			continue
		}

		// proper setting of EOF
		if nn == 0 && err == nil {
			err = io.EOF
		}
		return nn, err
	}
}

// ================================================================
func (c *socket) onReadFrom(p []byte, rflags int) (int, net.Addr, error) {
	// logger.Debugf("%s readFrom is requesting read-readiness ...", c.cid)
	err := <-c.submitReq(unix.EPOLL_CTL_MOD, unix.EPOLLIN, EV_READ)
	if err != nil {
		return 0, nil, err
	}
	// logger.Debugf("%s readFrom got a read-ready event ...", c.cid)
	return c.readFrom(p, rflags)
}

// ================================================================
func (c *socket) readFrom(p []byte, flags int) (int, net.Addr, error) {
	nn := 0
	for {
		n, sa, err := unix.Recvfrom(c.fd, p, flags)

		if n > 0 {
			nn += n
		} else if err == unix.EINTR {
			continue
		}

		// proper setting of EOF
		if nn == 0 && err == nil {
			err = io.EOF
		}
		return nn, sockaddrToUDP(sa), err
	}
}

// ================================================================
// write - is actually a one-shot write, the loop is just for handling
// the appearance of unix.EINTR error
// ================================================================
func (c *socket) write(p []byte) (int, error) {
	nn := 0
	for {
		n, err := unix.Write(c.fd, p)

		if n > 0 {
			nn += n
		} else if err == unix.EINTR {
			continue
		}
		if nn == len(p) {
			return nn, err
		}
		if err != nil {
			return nn, err
		}
		if n == 0 {
			return nn, io.ErrUnexpectedEOF
		}
		return nn, nil
	}
}

// ================================================================
func (c *socket) onWriteTo(p []byte, addr net.Addr) (int, error) {
	if addr == nil {
		return 0, fmt.Errorf("addr is nil")
	}
	err := <-c.submitReq(unix.EPOLL_CTL_MOD, unix.EPOLLOUT, EV_WRITE)
	if err != nil {
		return 0, err
	}
	return c.writeTo(p, addrToSockaddr(addr))
}

// ================================================================
func (c *socket) writeTo(p []byte, addr sockaddr) (int, error) {
	sa, err := addr.sockaddr()
	if err != nil {
		return 0, err
	}
	for {
		err := unix.Sendto(c.fd, p, 0, sa)
		if err == unix.EINTR {
			continue
		}

		if err != nil {
			return 0, err
		}
		return len(p), nil
	}
}

// ================================================================
func (s *socket) newSocket1() *socket1 {
	return &socket1{
		socket: s,
	}
}

// ================================================================
func (s *socket1) sendReadReq(dura time.Duration, data ...any) dtype.HdpEvent {
	req := newHdpEvent(data...)
	return s.sendReadReq1(dura, req)
}

// ================================================================
func (s *socket1) sendReadReq1(dura time.Duration, req dtype.HdpEvent) dtype.HdpEvent {
	readyCh := make(chan bool, 1)
	go func() {
		if dura != 0 {
			err := <-s.setDeadline(int(unix.EPOLLIN), dura)
			if err != nil {
				req.Respond(err)
				return
			}
		}
		<-readyCh
		// s.tpt[R] <- req
	}()
	req["readyCh"] = readyCh
	return req
}

// ================================================================
func (s *socket1) sendWriteReq(dura time.Duration, data ...any) dtype.HdpEvent {
	req := newHdpEvent(data...)
	return s.sendWriteReq1(dura, req)
}

// ================================================================
func (s *socket1) sendWriteReq1(dura time.Duration, req dtype.HdpEvent) dtype.HdpEvent {
	readyCh := make(chan bool, 1)
	go func() {
		if dura != 0 {
			err := <-s.setDeadline(int(unix.EPOLLOUT), dura)
			if err != nil {
				req.Respond(err)
				return
			}
		}
		<-readyCh
		// s.tpt[W] <- req
	}()
	req["readyCh"] = readyCh
	return req
}

// ================================================================
func (s *socket1) setDeadline(mode int, dura time.Duration) chan error {
	return epoller.SubmitIoReq(newHdpEvent(":data",
		"reqRef/cid", s.cid,
		"reqRef/fd", s.fd,
		"reqRef/flags", mode,
		"reqRef/deadline", time.Now().Add(dura)))
}

// ================================================================
func (c *socket) verifyChecksum(b []byte, crc uint16) error {
	if crc != crc16.Checksum(b, crc16.IBMTable) {
		return fmt.Errorf("%s checksum verification failed", c.cid) // unix.ECONNABORTED
	}
	return nil
}
