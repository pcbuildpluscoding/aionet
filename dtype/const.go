package dtype

const (
	C = 0
	U = 1
	R = 0
	W = 1
	E = 2
)

// ==================================================================//
// Activity
// ==================================================================//
type Activity int

const (
	Inactivity Activity = iota
	Aborted
	Cancelled
	Closed
	Complete
	Connected
	Continue
	Disconnected
	Init
	Ready
	Resolved
	Restarting
	Running
	Sigterm
	Started
	Starting
	Stopped
	Stopping
	Suspended
	Unresolved
	Waiting
)

func (c Activity) String() string {
	switch c {
	case 1:
		return "Aborted"
	case 2:
		return "Cancelled"
	case 3:
		return "Closed"
	case 4:
		return "Complete"
	case 5:
		return "Connected"
	case 6:
		return "Continue"
	case 7:
		return "Disconnected"
	case 8:
		return "Init"
	case 9:
		return "Ready"
	case 10:
		return "Resolved"
	case 11:
		return "Restarting"
	case 12:
		return "Running"
	case 13:
		return "Sigterm"
	case 14:
		return "Started"
	case 15:
		return "Starting"
	case 16:
		return "Stopped"
	case 17:
		return "Stopping"
	case 18:
		return "Suspended"
	case 19:
		return "Unresolved"
	case 20:
		return "Waiting"
	default:
		return "Inactivity"
	}
}

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

// ==================================================================//
// SerialType
// ==================================================================//
type SerialType int

const (
	t_nil SerialType = iota
	t_bool
	t_byteslice
	t_float32
	t_float64
	t_int
	t_int8
	t_int16
	t_int32
	t_int64
	t_list
	t_map
	t_string
	t_time
	t_uint
	t_uint8
	t_uint16
	t_uint32
	t_uint64
	t_flag1
	t_flag2
)

func (t SerialType) String() string {
	switch t {
	case t_nil:
		return "t_nil"
	case t_bool:
		return "t_bool"
	case t_byteslice:
		return "t_byteslice"
	case t_float32:
		return "t_float32"
	case t_float64:
		return "t_float64"
	case t_int:
		return "t_int"
	case t_int8:
		return "t_int8"
	case t_int16:
		return "t_int16"
	case t_int32:
		return "t_int32"
	case t_int64:
		return "t_int64"
	case t_list:
		return "t_list"
	case t_map:
		return "t_map"
	case t_string:
		return "t_string"
	case t_time:
		return "t_time"
	case t_uint:
		return "t_uint"
	case t_uint8:
		return "t_uint8"
	case t_uint16:
		return "t_uint16"
	case t_uint32:
		return "t_uint32"
	case t_uint64:
		return "t_uint64"
	case t_flag1:
		return "t_flag1"
	case t_flag2:
		return "t_flag2"
	default:
		return "t_invalid"
	}
}

// ==================================================================//
// HDP_STATE1
// ==================================================================//
type HDP_STATE1 int

const (
	HDP_UNDEFINED1 HDP_STATE1 = iota
	HDP_ACCEPT
	HDP_ACCEPT_ACK
	HDP_ACCEPTED
	HDP_CLOSED1
	HDP_CONNECT
	HDP_CONNECT_ACK
	HDP_CONNECTED
	HDP_DATAGRAM1
	HDP_DIAL
	HDP_ERROR1
	HDP_INIT1
	HDP_OPEN1
	HDP_RESET1
	HDP_STARTING
	HDP_STARTED
	HDP_QUERY
)

// ---------------------------------------------------------------//
// String
// ---------------------------------------------------------------//
func (s HDP_STATE1) String() string {
	switch s {
	case 0:
		return "HDP_UNDEFINED1"
	case 1:
		return "HDP_ACCEPT"
	case 2:
		return "HDP_ACCEPT_ACK"
	case 3:
		return "HDP_ACCEPTED"
	case 4:
		return "HDP_CLOSED1"
	case 5:
		return "HDP_CONNECT"
	case 6:
		return "HDP_CONNECT_ACK"
	case 7:
		return "HDP_CONNECTED"
	case 8:
		return "HDP_DATAGRAM1"
	case 9:
		return "HDP_DIAL"
	case 10:
		return "HDP_ERROR1"
	case 11:
		return "HDP_INIT1"
	case 12:
		return "HDP_OPEN1"
	case 13:
		return "HDP_RESET1"
	case 14:
		return "HDP_STARTING"
	case 15:
		return "HDP_STARTED"
	case 16:
		return "HDP_QUERY"
	default:
		return "error : invalid state"
	}
}

// ---------------------------------------------------------------//
// ReqConnect
// ---------------------------------------------------------------//
func (s HDP_STATE1) ReqConnect() bool {
	return s == HDP_CONNECT
}

// ---------------------------------------------------------------//
// AcknowConnect
// ---------------------------------------------------------------//
func (s HDP_STATE1) AcknowConnect() bool {
	return s == HDP_CONNECT_ACK
}

// ---------------------------------------------------------------//
// AcknowAccept
// ---------------------------------------------------------------//
func (s HDP_STATE1) AcknowAccept() bool {
	return s == HDP_ACCEPT_ACK
}

// ---------------------------------------------------------------//
// AcknowOpen
// ---------------------------------------------------------------//
func (s HDP_STATE1) AcknowOpen() bool {
	return s == HDP_CONNECTED
}

// ==================================================================//
// HDP_STATE2
// ==================================================================//
type HDP_STATE2 int

const (
	HDP_UNDEFINED HDP_STATE2 = iota
	HDP_CLOSE_ACK
	HDP_CLOSED
	HDP_CLOSING
	HDP_DATA_ACK
	HDP_DATAGRAM
	HDP_ERROR
	HDP_INIT
	HDP_INPUT
	HDP_NOACTION
	HDP_OPEN
	HDP_PULSE
	HDP_READ1
	HDP_READ2
	HDP_READY
	HDP_REPEAT
	HDP_RESET
	HDP_RMT_CLOSING
	HDP_STOPPED
	HDP_STOPPING
	HDP_TESTING
	HDP_UNRESOLVED
	HDP_WRITE1
	HDP_WRITE2
	HDP_QUERY2
)

// ---------------------------------------------------------------//
// String
// ---------------------------------------------------------------//
func (s HDP_STATE2) String() string {
	switch s {
	case 0:
		return "HDP_UNDEFINED"
	case 1:
		return "HDP_CLOSE_ACK"
	case 2:
		return "HDP_CLOSED"
	case 3:
		return "HDP_CLOSING"
	case 4:
		return "HDP_DATA_ACK"
	case 5:
		return "HDP_DATAGRAM"
	case 6:
		return "HDP_ERROR"
	case 7:
		return "HDP_INIT"
	case 8:
		return "HDP_INPUT"
	case 9:
		return "HDP_NOACTION"
	case 10:
		return "HDP_PULSE"
	case 11:
		return "HDP_OPEN"
	case 12:
		return "HDP_READ1"
	case 13:
		return "HDP_READ2"
	case 14:
		return "HDP_READY"
	case 15:
		return "HDP_REPEAT"
	case 16:
		return "HDP_RESET"
	case 17:
		return "HDP_RMT_CLOSING"
	case 18:
		return "HDP_STOPPED"
	case 19:
		return "HDP_STOPPING"
	case 20:
		return "HDP_TESTING"
	case 21:
		return "HDP_UNRESOLVED"
	case 22:
		return "HDP_WRITE1"
	case 23:
		return "HDP_WRITE2"
	case 24:
		return "HDP_QUERY2"
	default:
		return "error : invalid state"
	}
}

// ---------------------------------------------------------------//
// AckPeerClosing
// ---------------------------------------------------------------//
func (s HDP_STATE2) AckPeerClosing() bool {
	return s == HDP_CLOSE_ACK
}

// ---------------------------------------------------------------//
// PeerClosing
// ---------------------------------------------------------------//
func (s HDP_STATE2) PeerClosing() bool {
	return s == HDP_CLOSING
}

// ---------------------------------------------------------------//
// Closed
// ---------------------------------------------------------------//
func (s HDP_STATE2) Closed() bool {
	return s == HDP_CLOSED
}
