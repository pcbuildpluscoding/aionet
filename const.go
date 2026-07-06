package aionet

const (
	R = 0
	W = 1
	C = 2
	D = 3

	acknowConnect_ = 10
	onAccept_      = 11
	onAccepted_    = 12
	onConnectAck_  = 13
	onDial_        = 14
)

// ==================================================================//
// IoMode
// ==================================================================//
type ioMode int

const (
	EV_NULL ioMode = iota
	EV_ERROR
	EV_READ
	EV_READY
	EV_RESET
	EV_WRITE
)

// ---------------------------------------------------------------//
// String
// ---------------------------------------------------------------//
func (m ioMode) String() string {
	switch m {
	case 0:
		return "EV_NULL"
	case 1:
		return "EV_ERROR"
	case 2:
		return "EV_READ"
	case 3:
		return "EV_READY"
	case 4:
		return "EV_RESET"
	case 5:
		return "EV_WRITE"
	default:
		return "INVALID_MODE"
	}
}

var (
	maxEvents = 4096
	minEvents = 64

	ErrCodeWrongPeerRefNum = 1
	ErrReadFromNewConn     = 2
	ErrUnequalChecksum     = 3
	ErrUnexpectedReadFlag  = 4
	ErrConnectToNewConn    = 5
	ErrOnReadHeader        = 6
	ErrOnWriteHeader       = 7
	ErrWriteToNewConn      = 8
	ErrUnsupportedFlag     = 9
	ErrSeqNumOverflow      = 10
	ErrOnStateTransition   = 11
	ErrRingBufferFull      = 12
	// ErrWrongPeerRefNum     = &HDPError2{code: 1}
	ErrCancelled         = 13
	ErrTimeout           = 14
	ErrClosing           = 15
	ErrClosed            = 16
	ErrAborted           = 17
	ErrRefused           = 18
	ErrReset             = 19
	ErrOnClose           = 20
	ErrDialTimeout       = 21
	ErrConnectProto      = 22
	ErrSockOptTimeout    = 23
	ErrSockCreate        = 24
	ErrSockBind          = 25
	ErrEOF               = 26
	WarnPrePolledIoReady = 27

	VErrCancelled         = NewHdpError(ErrCancelled, "this")
	VErrTimeout           = NewHdpError(ErrTimeout, "this")
	VErrClosing           = NewHdpError(ErrClosing, "this")
	VErrClosed            = NewHdpError(ErrClosed, "this")
	VErrAborted           = NewHdpError(ErrAborted, "this")
	VErrRefused           = NewHdpError(ErrRefused, "this")
	VErrReset             = NewHdpError(ErrReset, "this")
	VErrEOF               = NewHdpError(ErrEOF, "this")
	VErrRingBufferFull    = NewHdpError(ErrRingBufferFull, "this")
	VWarnPrePolledIoReady = NewHdpError(WarnPrePolledIoReady, "this")
)
