package aionet

import (
	"context"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// ================================================================//
// socket
// ================================================================//
type socket1 struct {
	*socket
	cid string
	// state      [2]dtype.HDP_STATE1
	windowSize uint16
	suspended  bool
	// tpt        dtype.MultiCh
}

// ================================================================
func (s *socket1) bind() error {
	switch addr := s.laddr.(type) {
	case *net.UDPAddr:
		// We provide a socket that listens to a wildcard
		// address with reusable UDP port when the given laddr
		// is an appropriate UDP multicast address prefix.
		// This makes it possible for a single UDP listener to
		// join multiple different group addresses, for
		// multiple UDP listeners that listen on the same UDP
		// port to join the same group address.
		if addr.IP != nil && addr.IP.IsMulticast() {
			if err := setDefaultMulticastSockopts(s.fd); err != nil {
				return err
			}
			addr := *addr
			switch s.family {
			case unix.AF_INET:
				addr.IP = net.IPv4zero
			case unix.AF_INET6:
				addr.IP = net.IPv6unspecified
			}
			s.laddr = &addr
		}
	}

	addr := sockAddr{s.laddr}
	lsa, err := addr.sockaddr()
	if err != nil {
		return err
	}

	if err = unix.Bind(s.fd, lsa); err != nil {
		return os.NewSyscallError("bind", err)
	}

	lsa, _ = unix.Getsockname(s.fd)
	s.laddr = sockaddrToUDP(lsa)

	logger.Debugf("%s is starting ...", s.cid)
	err = s.init(s.cid)
	logger.Debugf("%s is started.", s.cid)
	return err
}

// ================================================================
func (c *socket1) bindToAddr(addr sockaddr) error {
	sa, err := addr.sockaddr()
	if err != nil {
		return err
	}

	if err = unix.Bind(c.fd, sa); err != nil {
		return NewHdpError(ErrSockBind, c.cid, err)
	}

	lsa, _ := unix.Getsockname(c.fd)
	c.laddr = sockaddrToUDP(lsa)
	return nil
}

// ================================================================
func (c *socket1) checkConnected() error {
	nerr, err := unix.GetsockoptInt(c.fd, unix.SOL_SOCKET, unix.SO_ERROR)
	if err != nil {
		return NewHdpError(ErrSockCreate, c.cid, err)
	}
	switch err := unix.Errno(nerr); err {
	case unix.EINPROGRESS, unix.EALREADY, unix.EINTR:
	case unix.EISCONN:
		return c.confirmConnected()
	case unix.Errno(0):
		// The runtime poller can wake us up spuriously;
		// see issues 14548 and 19289. Check that we are
		// really connected; if not, wait again.
		return c.confirmConnected()
	default:
		return NewHdpError(ErrSockCreate, c.cid, err)
	}
	return nil
}

// ================================================================
func (c *socket1) confirmConnected() error {
	sa, err := unix.Getpeername(c.fd)
	if err != nil {
		return err
	}
	c.raddr = sockaddrToUDP(sa)

	sa, err = unix.Getsockname(c.fd)
	if err != nil {
		return err
	}

	c.laddr = sockaddrToUDP(sa)
	// c.state = Connected
	return nil
}

// ================================================================
func (c *socket1) connect(raddr sockaddr, timeout time.Duration) error {
	var err error
	deadline, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go c.connectToPeer(raddr, errCh)

	select {
	case <-deadline.Done():
		return NewHdpError(ErrDialTimeout, c.cid, raddr.String())
	case err = <-errCh:
	}

	switch err {
	case unix.EINPROGRESS, unix.EALREADY, unix.EINTR:
	case nil, unix.EISCONN:
		return c.confirmConnected()
	case unix.EINVAL:
		return NewHdpError(ErrConnectProto, c.cid, err)
	}

	for {
		select {
		case err := <-c.submitReq(c.cid, unix.EPOLL_CTL_MOD, unix.EPOLLOUT, EV_WRITE):
			if err != nil {
				return err
			}
		case <-deadline.Done():
			return NewHdpError(ErrSockOptTimeout, c.cid)
		}
		err := c.checkConnected()
		if err != nil {
			return err
		}
	}
}

// ================================================================
func (c *socket1) connectToPeer(raddr sockaddr, errCh chan error) {
	rsa, err := raddr.sockaddr()
	if err != nil {
		errCh <- err
	}

	errCh <- unix.Connect(c.fd, rsa)
}
