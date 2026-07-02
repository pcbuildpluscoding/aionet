package aionet

import (
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// Boolean to int.
func boolint(b bool) int {
	if b {
		return 1
	}
	return 0
}

// socket returns a network file descriptor that is ready for
// asynchronous I/O using the network poller.
// func socket(ctx context.Context, net string, family, sotype, proto int, ipv6only bool, laddr, raddr sockaddr, ctrlCtxFn func(context.Context, string, string, unix.RawConn) error) (int, error) {
func newSocketFd(family, sotype, proto int, ipv6only bool) (int, error) {
	fd, err := unix.Socket(family, sotype|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, proto)
	if err != nil {
		return -1, os.NewSyscallError("socket", err)
	}
	return fd, setDefaultSockopts(fd, family, sotype, ipv6only)
}

func setDefaultSockopts(s, family, sotype int, ipv6only bool) error {
	var err error
	if family == unix.AF_INET6 && sotype != unix.SOCK_RAW {
		// Allow both IP versions even if the OS default
		// is otherwise. Note that some operating systems
		// never admit this option.
		err = unix.SetsockoptInt(s, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY, boolint(ipv6only))
	}
	if (sotype == unix.SOCK_DGRAM || sotype == unix.SOCK_RAW) && family != unix.AF_UNIX {
		// Allow broadcast.
		return os.NewSyscallError("setsockopt", unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_BROADCAST, 1))
	}
	return err
}

// A sockaddr represents a TCP, UDP, IP or Unix network endpoint
// address that can be converted into a unix.Sockaddr.
type sockaddr interface {
	net.Addr

	// family returns the platform-dependent address family
	// identifier.
	family() int

	// isWildcard reports whether the address is a wildcard
	// address.
	isWildcard() bool

	// sockaddr returns the address converted into a syscall
	// sockaddr type that implements unix.Sockaddr
	// interface. It returns a nil interface when the address is
	// nil.
	sockaddr() (unix.Sockaddr, error)

	// toLocal maps the zero address to a local system address (127.0.0.1 or ::1)
	toLocal(net string) sockaddr

	toAddr() net.Addr
}

type sockAddr struct {
	net.Addr
}

func (a *sockAddr) family() int {
	switch addr := a.Addr.(type) {
	case *net.TCPAddr:
		if addr == nil || len(addr.IP) <= net.IPv4len {
			return unix.AF_INET
		}
		if addr.IP.To4() != nil {
			return unix.AF_INET
		}
		return unix.AF_INET6
	case *net.UDPAddr:
		if addr == nil || len(addr.IP) <= net.IPv4len {
			return unix.AF_INET
		}
		if addr.IP.To4() != nil {
			return unix.AF_INET
		}
		return unix.AF_INET6
	}
	return 0
}

// ---------------------------------------------------------------//
// isWildcard
// ---------------------------------------------------------------//
func (a *sockAddr) isWildcard() bool {
	switch addr := a.Addr.(type) {
	case *net.TCPAddr:
		if addr == nil || addr.IP == nil {
			return true
		}
		return addr.IP.IsUnspecified()
	case *net.UDPAddr:
		if addr == nil || addr.IP == nil {
			return true
		}
		return addr.IP.IsUnspecified()
	}
	return false
}

// ---------------------------------------------------------------//
// unwrap
// ---------------------------------------------------------------//
func (a *sockAddr) unwrap() (net.IP, int) {
	switch addr := a.Addr.(type) {
	case *net.TCPAddr:
		return addr.IP, addr.Port
	case *net.UDPAddr:
		return addr.IP, addr.Port
	}
	return net.IP{}, 0
}

// ---------------------------------------------------------------//
// unwrap6
// ---------------------------------------------------------------//
func (a *sockAddr) unwrap6() (net.IP, int, string) {
	switch addr := a.Addr.(type) {
	case *net.TCPAddr:
		return addr.IP, addr.Port, ""
	case *net.UDPAddr:
		return addr.IP, addr.Port, ""
	}
	return net.IP{}, 0, ""
}

// ---------------------------------------------------------------//
// sockaddr
// ---------------------------------------------------------------//
func (a *sockAddr) sockaddr() (unix.Sockaddr, error) {
	switch a.family() {
	case unix.AF_INET:
		sa, err := ipToSockaddrInet4(a.unwrap())
		if err != nil {
			return nil, err
		}
		return &sa, nil
	}
	return nil, &net.AddrError{Err: "invalid address family", Addr: a.String()}
}

// ---------------------------------------------------------------//
// toLocal
// ---------------------------------------------------------------//
func (a *sockAddr) toLocal(net string) sockaddr {
	return a
}

// ---------------------------------------------------------------//
// toAddr
// ---------------------------------------------------------------//
func (a *sockAddr) toAddr() net.Addr {
	return a.Addr
}

// ---------------------------------------------------------------//
// ipToSockaddrInet4
// ---------------------------------------------------------------//
func ipToSockaddrInet4(ip net.IP, port int) (unix.SockaddrInet4, error) {
	if len(ip) == 0 {
		ip = net.IPv4zero
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return unix.SockaddrInet4{}, &net.AddrError{Err: "non-IPv4 address", Addr: ip.String()}
	}
	sa := unix.SockaddrInet4{Port: port}
	copy(sa.Addr[:], ip4)
	return sa, nil
}

// wrapSyscallError takes an error and a syscall name. If the error is
// a unix.Errno, it wraps it in an os.SyscallError using the syscall name.
func wrapSyscallError(name string, err error) error {
	if _, ok := err.(unix.Errno); ok {
		err = os.NewSyscallError(name, err)
	}
	return err
}

func addrToSockaddr(addr net.Addr) sockaddr {
	return &sockAddr{Addr: addr}
}

func sockaddrToUDP(sa unix.Sockaddr) *net.UDPAddr {
	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.UDPAddr{IP: sa.Addr[0:], Port: sa.Port}
	case *unix.SockaddrInet6:
		return &net.UDPAddr{IP: sa.Addr[0:], Port: sa.Port}
	}
	return &net.UDPAddr{}
}

// favoriteAddrFamily returns the appropriate address family for the
// given network, laddr, raddr and mode.
//
// If mode indicates "listen" and laddr is a wildcard, we assume that
// the user wants to make a passive-open connection with a wildcard
// address family, both AF_INET and AF_INET6, and a wildcard address
// like the following:
//
//   - A listen for a wildcard communication domain, "tcp" or
//     "udp", with a wildcard address: If the platform supports
//     both IPv6 and IPv4-mapped IPv6 communication capabilities,
//     or does not support IPv4, we use a dual stack, AF_INET6 and
//     IPV6_V6ONLY=0, wildcard address listen. The dual stack
//     wildcard address listen may fall back to an IPv6-only,
//     AF_INET6 and IPV6_V6ONLY=1, wildcard address listen.
//     Otherwise we prefer an IPv4-only, AF_INET, wildcard address
//     listen.
//
//   - A listen for a wildcard communication domain, "tcp" or
//     "udp", with an IPv4 wildcard address: same as above.
//
//   - A listen for a wildcard communication domain, "tcp" or
//     "udp", with an IPv6 wildcard address: same as above.
//
//   - A listen for an IPv4 communication domain, "tcp4" or "udp4",
//     with an IPv4 wildcard address: We use an IPv4-only, AF_INET,
//     wildcard address listen.
//
//   - A listen for an IPv6 communication domain, "tcp6" or "udp6",
//     with an IPv6 wildcard address: We use an IPv6-only, AF_INET6
//     and IPV6_V6ONLY=1, wildcard address listen.
//
// Otherwise guess: If the addresses are IPv4 then returns AF_INET,
// or else returns AF_INET6. It also returns a boolean value what
// designates IPV6_V6ONLY option.
//
// Note that the latest DragonFly BSD and OpenBSD kernels allow
// neither "net.inet6.ip6.v6only=1" change nor IPPROTO_IPV6 level
// IPV6_V6ONLY socket option setting.
func favoriteAddrFamily(network string, laddr, raddr sockaddr, mode string) (family int, ipv6only bool) {
	switch network[len(network)-1] {
	case '4':
		return unix.AF_INET, false
	case '6':
		return unix.AF_INET6, true
	}

	if mode == "listen" && (laddr == nil || laddr.isWildcard()) {
		if supportsIPv4map() || !supportsIPv4() {
			return unix.AF_INET6, false
		}
		if laddr == nil {
			return unix.AF_INET, false
		}
		return laddr.family(), false
	}

	if (laddr == nil || laddr.family() == unix.AF_INET) &&
		(raddr == nil || raddr.family() == unix.AF_INET) {
		return unix.AF_INET, false
	}
	return unix.AF_INET6, false
}

// supportsIPv4map reports whether the platform supports mapping an
// IPv4 address inside an IPv6 address at transport layer
// protocols. See RFC 4291, RFC 4038 and RFC 3493.
func supportsIPv4map() bool {
	// Some operating systems provide no support for mapping IPv4
	// addresses to IPv6, and a runtime check is unnecessary.
	return false
}

// supportsIPv4 reports whether the platform supports IPv4 networking
// functionality.
func supportsIPv4() bool {
	return true
}

func setDefaultListenerSockopts(s int) error {
	// Allow reuse of recently-used addresses.
	return os.NewSyscallError("setsockopt", unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1))
}

func setDefaultMulticastSockopts(s int) error {
	// Allow multicast UDP and raw IP datagram sockets to listen
	// concurrently across multiple listeners.
	return os.NewSyscallError("setsockopt", unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1))
}
