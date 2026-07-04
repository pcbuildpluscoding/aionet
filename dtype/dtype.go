package dtype

import (
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/runtime/protoimpl"
	spb "google.golang.org/protobuf/types/known/structpb"
)

type HdpEventA map[string]any
type HdpEvent map[string]any
type Void struct{}

// ---------------------------------------------------------------//
// Args
// ---------------------------------------------------------------//
func (r HdpEvent) Args() []any {
	args := []any{}
	for k, v := range r {
		args = append(args, k, v)
	}
	return args
}

// ---------------------------------------------------------------//
// add
// ---------------------------------------------------------------//
func (r HdpEvent) add(args ...any) error {
	var (
		key string
		ok  bool
	)
	for i, arg := range args {
		switch i % 2 {
		case 0:
			if key, ok = arg.(string); !ok {
				return fmt.Errorf("key string, value any sequence format is required.")
			}
		default:
			r[key] = arg
		}
	}
	return nil
}

// ---------------------------------------------------------------//
// With
// ---------------------------------------------------------------//
func (r HdpEvent) Add(args ...any) error {
	for i, arg := range args {
		switch val := arg.(type) {
		case nil:
			r["error"] = nil
		case Activity:
			r["activity"] = val
		case error:
			r["error"] = val
		case chan HdpEvent:
			r["resultCh"] = val
		case MultiCh:
			r["multiCh"] = val
		case HDP_STATE1:
			// logger.Debugf("$$$$$$$$$ got flag1 : %d, %s", int(val), val.String())
			r["flag1"] = val
		case HDP_STATE2:
			// logger.Debugf("$$$$$$$$$ got flag2 : %d, %s", int(val), val.String())
			r["flag2"] = val
		case string:
			if val == ":data" {
				if j := i + 1; j < len(args) {
					return r.add(args[j:]...)
				}
			}
		default:
		}
	}
	return nil
}

// ---------------------------------------------------------------//
// AddCh
// ---------------------------------------------------------------//
func (r HdpEvent) AddCh(args ...any) chan HdpEvent {
	err := r.Add(args...)
	if err != nil {
		r["error"] = err
	}
	ch := make(chan HdpEvent, 1)
	r["resultCh"] = ch
	return ch
}

// ---------------------------------------------------------------//
// Async
// ---------------------------------------------------------------//
func (r HdpEvent) Async() {
	readyCh, _ := r["readyCh"].(chan bool)
	if readyCh != nil {
		readyCh <- true
	}
	// delete the ready channel for synchronous request timing
	delete(r, "readyCh")
}

// ---------------------------------------------------------------//
// Conn
// ---------------------------------------------------------------//
func (r HdpEvent) Conn(arg ...string) net.Conn {
	key := "conn"
	if len(arg) > 0 {
		key = arg[0]
	}
	switch x := r[key]; x {
	case nil:
	default:
		switch conn := x.(type) {
		case net.Conn:
			return conn
		}
	}
	return nil
}

// ---------------------------------------------------------------//
// Copy
// ---------------------------------------------------------------//
func (r HdpEvent) Copy(args ...any) HdpEvent {
	c := HdpEvent{}
	for k, v := range r {
		// todo : what if the value is a slice, map or pointer ?
		c[k] = v
	}
	return c.With1(args...)
}

// ---------------------------------------------------------------//
// Delete
// ---------------------------------------------------------------//
func (r HdpEvent) Delete(args ...string) HdpEvent {
	for _, key := range args {
		delete(r, key)
	}
	return r
}

// ---------------------------------------------------------------//
// Decode
// ---------------------------------------------------------------//
func (r HdpEvent) Decode(b []byte) (HdpEvent, error) {
	s := spb.Struct{}
	err := s.UnmarshalJSON(b)
	if err != nil {
		return r, err
	}

	ftype := map[string]int{}
	fields := []string{}
	for k := range s.Fields {
		if strings.HasSuffix(k, "/type") {
			fname := strings.Split(k, "/type")[0]
			ftype[fname] = int(s.Fields[k].GetNumberValue())
			fields = append(fields, fname)
		}
	}
	//fmt.Printf("HdpEvent.Decode got fieldset : %v\n", fields)
	for _, fname := range fields {
		tname := ftype[fname]
		r[fname] = convertValue(tname, s.Fields[fname])
	}
	return r, nil
}

// ---------------------------------------------------------------//
// convertValue
// ---------------------------------------------------------------//
func convertValue(svalue int, value *spb.Value) any {
	serialType := SerialType(svalue)
	// fmt.Printf("got serialType : %s\n", serialType.String())
	switch serialType {
	case t_bool:
		if x, ok := value.Kind.(*spb.Value_BoolValue); ok {
			return x
		}
	case t_byteslice:
		if x, ok := value.Kind.(*spb.Value_StringValue); ok {
			bslice, err := base64.StdEncoding.DecodeString(x.StringValue)
			if err != nil {
				return fmt.Errorf("base64 byteslice decoding failed : %v", err)
			}
			return bslice
		}
	case t_float32:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return float32(x.NumberValue)
		}
	case t_float64:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return x.NumberValue
		}
	case t_int:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return int(x.NumberValue)
		}
	case t_int8:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return int8(x.NumberValue)
		}
	case t_int16:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return int16(x.NumberValue)
		}
	case t_int32:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return int32(x.NumberValue)
		}
	case t_int64:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return int64(x.NumberValue)
		}
	case t_list:
		if x, ok := value.Kind.(*spb.Value_ListValue); ok {
			return x.ListValue.AsSlice()
		}
	case t_map:
		if x, ok := value.Kind.(*spb.Value_StructValue); ok {
			return x.StructValue.AsMap()
		}
	case t_nil:
		return nil
	case t_string:
		if x, ok := value.Kind.(*spb.Value_StringValue); ok {
			return x.StringValue
		}
	case t_time:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return time.UnixMicro(int64(x.NumberValue))
		}
	case t_uint:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return uint(x.NumberValue)
		}
	case t_uint8:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return uint8(x.NumberValue)
		}
	case t_uint16:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return uint16(x.NumberValue)
		}
	case t_uint32:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return uint32(x.NumberValue)
		}
	case t_uint64:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return uint64(x.NumberValue)
		}
	case t_flag1:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return HDP_STATE1(x.NumberValue)
		}
	case t_flag2:
		if x, ok := value.Kind.(*spb.Value_NumberValue); ok {
			return HDP_STATE2(x.NumberValue)
		}
	}
	return fmt.Errorf("%T is not deserializable", value.AsInterface())
}

// ---------------------------------------------------------------//
// Encode
// ---------------------------------------------------------------//
func (r HdpEvent) Encode() ([]byte, error) {
	x := map[string]*spb.Value{}
	for k, iv := range r {
		switch v := iv.(type) {
		case nil:
			x[k] = &spb.Value{Kind: &spb.Value_NullValue{NullValue: spb.NullValue_NULL_VALUE}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_nil)}}
		case bool:
			x[k] = &spb.Value{Kind: &spb.Value_BoolValue{BoolValue: v}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_bool)}}
		case int:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_int)}}
		case int8:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_int8)}}
		case int16:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_int16)}}
		case int32:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_int32)}}
		case int64:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_int64)}}
		case uint:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_uint)}}
		case uint8:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_uint8)}}
		case uint16:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_uint16)}}
		case uint32:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_uint32)}}
		case uint64:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_uint64)}}
		case float32:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_float32)}}
		case float64:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: v}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_float64)}}
		case string:
			if !utf8.ValidString(v) {
				return nil, protoimpl.X.NewError("invalid UTF-8 in string: %q", v)
			}
			x[k] = &spb.Value{Kind: &spb.Value_StringValue{StringValue: v}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_string)}}
		case time.Time:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(v.UnixMicro())}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_time)}}
		case []byte:
			x[k] = &spb.Value{Kind: &spb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(v)}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_byteslice)}}
		case map[string]any:
			s, err := spb.NewStruct(v)
			if err != nil {
				return nil, err
			}
			x[k] = &spb.Value{Kind: &spb.Value_StructValue{StructValue: s}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_map)}}
		case []any:
			l, err := spb.NewList(v)
			if err != nil {
				return nil, err
			}
			x[k] = &spb.Value{Kind: &spb.Value_ListValue{ListValue: l}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_list)}}
		case HDP_STATE1:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(int(v))}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_flag1)}}
		case HDP_STATE2:
			x[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(int(v))}}
			x[k+"/type"] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(t_flag2)}}
		default:
			if !strings.HasSuffix(k, "eventCh") {
				panic(fmt.Errorf("%s type %T is unserializable", k, iv))
			}
		}
	}
	s := spb.Struct{
		Fields: x,
	}
	return s.MarshalJSON()
}

// ---------------------------------------------------------------//
// HasAny
// ---------------------------------------------------------------//
func (r HdpEvent) HasAny(args ...string) bool {
	for _, arg := range args {
		if _, ok := r[arg]; ok {
			return ok
		}
	}
	return false
}

// ---------------------------------------------------------------//
// HasKeys
// ---------------------------------------------------------------//
func (r HdpEvent) HasKeys(args ...string) bool {
	for _, arg := range args {
		if _, ok := r[arg]; !ok {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------//
// Merge
// ---------------------------------------------------------------//
func (r HdpEvent) Merge(x HdpEvent, args ...string) HdpEvent {
	if args == nil {
		for key := range x {
			r[key] = x[key]
		}
	} else {
		for _, key := range args {
			r[key] = x[key]
		}
	}
	return r
}

// ---------------------------------------------------------------//
// ResetWith
// ---------------------------------------------------------------//
func (r HdpEvent) New(args ...any) HdpEvent {
	x := HdpEvent{}
	return x.With(args...)
}

// ---------------------------------------------------------------//
// Respond
// ---------------------------------------------------------------//
func (r HdpEvent) Respond(args ...any) {
	// res := HdpEvent{}
	// res.With(args...)
	altCh := r.Ch("altCh")
	ch := r.Ch()
	// logger.Debugf("HdpEvent got caller result channel and altCh : %v, %v", ch, altCh)
	if ch != nil {
		ch <- r
	} else if altCh != nil {
		r.Delete("altCh")
		altCh <- r
	}
}

// ---------------------------------------------------------------//
// Respond1
// ---------------------------------------------------------------//
func (r HdpEvent) Respond1(res HdpEvent, altCh chan HdpEvent) {
	ch := r.Ch()
	// logger.Debugf("HdpEvent got caller result channel and altCh : %v, %v", ch, altCh)
	if ch != nil {
		ch <- res
	} else if altCh != nil {
		altCh <- res
	}
}

// ---------------------------------------------------------------//
// Retval
// ---------------------------------------------------------------//
func (r HdpEvent) Retval(arg ...string) (int, error) {
	key := "byteNum"
	if arg != nil {
		key = arg[0]
	}
	return r.Int(key), r.Err()
}

// ---------------------------------------------------------------//
// Set
// ---------------------------------------------------------------//
func (r HdpEvent) Set(key string, value any) {
	r[key] = value
}

// ---------------------------------------------------------------//
// Sync
// ---------------------------------------------------------------//
func (r HdpEvent) Sync() chan HdpEvent {
	ch := make(chan HdpEvent, 1)
	r["resultCh"] = ch
	readyCh, _ := r["readyCh"].(chan bool)
	if readyCh == nil {
		return ch
	}
	readyCh <- true
	delete(r, "readyCh")
	return ch
}

// ---------------------------------------------------------------//
// Params
// ---------------------------------------------------------------//
func (r HdpEvent) Value(key string) any {
	return r[key]
}

// ---------------------------------------------------------------//
// With
// ---------------------------------------------------------------//
func (r HdpEvent) With(args ...any) HdpEvent {
	if r == nil {
		r = HdpEvent{}
	}
	err := r.Add(args...)
	if err != nil {
		r["error"] = err
	}
	return r
}

// ---------------------------------------------------------------//
// Withc
// ---------------------------------------------------------------//
func (r HdpEvent) Withc(code int, args ...any) HdpEvent {
	if r == nil {
		r = HdpEvent{}
	}
	r["code"] = code
	for _, arg := range args {
		switch val := arg.(type) {
		case string:
			r["status"] = val
		case error:
			r["error"] = val
		}
	}
	return r
}

// ---------------------------------------------------------------//
// With
// ---------------------------------------------------------------//
func (r HdpEvent) With1(args ...any) HdpEvent {
	flag := r["flag1"]
	r.With(args...)
	if flag != r["flag1"] {
		r["auxFlag"] = flag
	} else if r["auxFlag"] != nil {
		r["flag1"] = r["auxFlag"]
		r["auxFlag"] = nil
	}
	return r
}

// ---------------------------------------------------------------//
// With
// ---------------------------------------------------------------//
func (r HdpEvent) With2(args ...any) HdpEvent {
	flag := r["flag2"]
	r.With(args...)
	if flag != r["flag2"] {
		r["auxFlag"] = flag
	} else if r["auxFlag"] != nil {
		r["flag2"] = r["auxFlag"]
		r["auxFlag"] = nil
	}
	return r
}

// ----------------------------------------------------------------//
// Withf
// ----------------------------------------------------------------//
func (r HdpEvent) Withf(code int, format string, args ...interface{}) HdpEvent {
	r["code"] = code
	if code >= 400 {
		r["error"] = fmt.Errorf(format, args...)
	} else {
		r["desc"] = fmt.Sprintf(format, args...)
	}
	return r
}

// ==================================================================//
// fdSet
// ==================================================================//
type fdSet struct {
	err error
	fd  uint32
	cid string
}

func (s fdSet) Print() string {
	return fmt.Sprintf("mocket fd endpoints : %d, %s", s.fd, s.cid)
}

func (s fdSet) Error() string {
	return fmt.Sprintf("mocket init error : %v", s.err)
}

func (s fdSet) Tell() (uint32, string) {
	return s.fd, s.cid
}

func (s *fdSet) ToValue() (*spb.Value, error) {
	x := map[string]any{
		"connex/fd":  s.fd,
		"connex/cid": s.cid,
	}
	logger.Debugf("creating a DbRequest.action:Init response : %v ...", x)
	return spb.NewValue(x)
}

// ==================================================================//
// frame
// ==================================================================//
type frame struct {
	B []byte
	N int
}

// ==================================================================
func (f *frame) Value() (int, []byte) {
	return f.N, f.B
}

// ==================================================================//
// eventCh
// ==================================================================//
type EventCh chan HdpEvent

// ---------------------------------------------------------------//
// Put
// ---------------------------------------------------------------//
func (ch EventCh) Put(args ...any) {
	req := HdpEvent{"Put": true}
	req.Add(args...)
	ch <- req
}

// ---------------------------------------------------------------//
// Put1
// ---------------------------------------------------------------//
func (ch EventCh) Put1(req HdpEvent) {
	req["Put"] = true
	ch <- req
}

// ---------------------------------------------------------------//
// Get
// ---------------------------------------------------------------//
func (ch EventCh) Get(keys ...string) EventCh {
	req := HdpEvent{}
	ch1 := req.AddCh()
	switch {
	case len(keys) == 0:
		req["GetAll"] = true
	default:
		req["Get"] = true
		for _, key := range keys {
			req[key] = nil
		}
	}
	ch <- req
	return ch1
}

// ==================================================================//
// Performer
// ==================================================================//
type Performer func([][]byte)

// ==================================================================//
// Serializer
// ==================================================================//
type Serializer interface {
	ToValue(...string) (spb.Value, error)
	FromValue(*spb.Value) error
	TypeName() string
}

type requestID struct{}

// ==================================================================//
// fdHolder
// ==================================================================//
type fdHolder interface {
	Tell() (uint32, string)
}

// ==================================================================//
// Statet
// ==================================================================//
type Statet func(HdpEvent) (Statet, HdpEvent)
type Stateh map[uint16]Statet
