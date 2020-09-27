// Copyright 2020 The Libc Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libc // import "modernc.org/libc"

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"modernc.org/libc/errno"
	"modernc.org/libc/signal"
	"modernc.org/libc/sys/types"
)

const (
	allocatorPageOverhead = 4 * unsafe.Sizeof(int(0))
	stackHeaderSize       = unsafe.Sizeof(stackHeader{})
	stackSegmentSize      = 1<<12 - allocatorPageOverhead
	uintptrSize           = unsafe.Sizeof(uintptr(0))
)

var (
	Covered  = map[uintptr]struct{}{}
	CoveredC = map[string]struct{}{}
	fToken   uintptr
	tid      int32
	atExit   []func()

	signals   [signal.NSIG]uintptr
	signalsMu sync.Mutex

	objectMu sync.Mutex
	objects  = map[uintptr]interface{}{}

	_ = origin
	_ = trc
)

func origin(skip int) string {
	pc, fn, fl, _ := runtime.Caller(skip)
	f := runtime.FuncForPC(pc)
	var fns string
	if f != nil {
		fns = f.Name()
		if x := strings.LastIndex(fns, "."); x > 0 {
			fns = fns[x+1:]
		}
	}
	return fmt.Sprintf("%s:%d:%s", filepath.Base(fn), fl, fns)
}

func trc(s string, args ...interface{}) string { //TODO-
	switch {
	case s == "":
		s = fmt.Sprintf(strings.Repeat("%v ", len(args)), args...)
	default:
		s = fmt.Sprintf(s, args...)
	}
	_, fn, fl, _ := runtime.Caller(1)
	r := fmt.Sprintf("\n%s:%d: TRC %s", fn, fl, s)
	fmt.Fprintf(os.Stdout, "%s\n", r)
	os.Stdout.Sync()
	return r
}

func todo(s string, args ...interface{}) string { //TODO-
	switch {
	case s == "":
		s = fmt.Sprintf(strings.Repeat("%v ", len(args)), args...)
	default:
		s = fmt.Sprintf(s, args...)
	}
	r := fmt.Sprintf("%s: TODOTODO %s", origin(2), s) //TODOOK
	if dmesgs {
		dmesg("%s", r)
	}
	fmt.Fprintf(os.Stdout, "%s\n", r)
	fmt.Fprintf(os.Stdout, "%s\n", debug.Stack()) //TODO-
	os.Stdout.Sync()
	os.Exit(1)
	panic("unrechable")
}

var coverPCs [1]uintptr //TODO not concurrent safe

func Cover() {
	runtime.Callers(2, coverPCs[:])
	Covered[coverPCs[0]] = struct{}{}
}

func CoverReport(w io.Writer) error {
	var a []string
	pcs := make([]uintptr, 1)
	for pc := range Covered {
		pcs[0] = pc
		frame, _ := runtime.CallersFrames(pcs).Next()
		a = append(a, fmt.Sprintf("%s:%07d:%s", filepath.Base(frame.File), frame.Line, frame.Func.Name()))
	}
	sort.Strings(a)
	_, err := fmt.Fprintf(w, "%s\n", strings.Join(a, "\n"))
	return err
}

func CoverC(s string) {
	CoveredC[s] = struct{}{}
}

func CoverCReport(w io.Writer) error {
	var a []string
	for k := range CoveredC {
		a = append(a, k)
	}
	sort.Strings(a)
	_, err := fmt.Fprintf(w, "%s\n", strings.Join(a, "\n"))
	return err
}

func token() uintptr { return atomic.AddUintptr(&fToken, 1) }

func addObject(o interface{}) uintptr {
	t := token()
	objectMu.Lock()
	objects[t] = o
	objectMu.Unlock()
	return t
}

func getObject(t uintptr) interface{} {
	objectMu.Lock()
	o := objects[t]
	if o == nil {
		panic(todo("", t))
	}

	objectMu.Unlock()
	return o
}

func removeObject(t uintptr) {
	objectMu.Lock()
	if _, ok := objects[t]; !ok {
		panic(todo(""))
	}

	delete(objects, t)
	objectMu.Unlock()
}

type TLS struct {
	ID     int32
	errnop uintptr
	stack  stackHeader
}

func NewTLS() *TLS {
	id := atomic.AddInt32(&tid, 1)
	t := &TLS{ID: id}
	t.errnop = mustCalloc(t, types.Size_t(unsafe.Sizeof(int32(0))))
	return t
}

func (t *TLS) setErrno(err interface{}) { //TODO -> etc.go
	if dmesgs {
		dmesg("%v: %T(%v)\n%s", origin(1), err, err, debug.Stack())
	}
again:
	switch x := err.(type) {
	case int:
		*(*int32)(unsafe.Pointer(t.errnop)) = int32(x)
	case int32:
		*(*int32)(unsafe.Pointer(t.errnop)) = x
	case *os.PathError:
		err = x.Err
		goto again
	case syscall.Errno:
		*(*int32)(unsafe.Pointer(t.errnop)) = int32(x)
	case *os.SyscallError:
		err = x.Err
		goto again
	default:
		panic(todo("%T", x))
	}
}

func (t *TLS) Close() {
	Xfree(t, t.errnop)
}

func (t *TLS) Alloc(n int) (r uintptr) {
	n += 15
	n &^= 15
	if t.stack.free >= n {
		r = t.stack.sp
		t.stack.free -= n
		t.stack.sp += uintptr(n)
		return r
	}

	if t.stack.page != 0 {
		*(*stackHeader)(unsafe.Pointer(t.stack.page)) = t.stack
	}
	rq := n + int(stackHeaderSize)
	if rq < int(stackSegmentSize) {
		rq = int(stackSegmentSize)
	}
	t.stack.free = rq - int(stackHeaderSize)
	t.stack.prev = t.stack.page
	rq += 15
	rq &^= 15
	t.stack.page = mustMalloc(t, types.Size_t(rq))
	t.stack.sp = t.stack.page + stackHeaderSize

	r = t.stack.sp
	t.stack.free -= n
	t.stack.sp += uintptr(n)
	return r
}

func (t *TLS) Free(n int) {
	n += 15
	n &^= 15
	t.stack.free += n
	t.stack.sp -= uintptr(n)
	if t.stack.sp != t.stack.page+stackHeaderSize {
		return
	}

	Xfree(t, t.stack.page)
	if t.stack.prev != 0 {
		t.stack = *(*stackHeader)(unsafe.Pointer(t.stack.prev))
		return
	}

	t.stack = stackHeader{}
}

type stackHeader struct {
	free int     // bytes left in page
	page uintptr // stack page
	prev uintptr // prev stack page = prev stack header
	sp   uintptr // next allocation address
}

func cString(t *TLS, s string) uintptr {
	n := len(s)
	p := mustMalloc(t, types.Size_t(n)+1)
	copy((*RawMem)(unsafe.Pointer(p))[:n:n], s)
	*(*byte)(unsafe.Pointer(p + uintptr(n))) = 0
	return p
}

func mustMalloc(t *TLS, n types.Size_t) uintptr {
	if p := Xmalloc(t, n); p != 0 {
		return p
	}

	panic("OOM")
}

// VaList fills a varargs list at p with args and returns uintptr(p).  The list
// must have been allocated by caller and it must not be in Go managed
// memory, ie. it must be pinned. Caller is responsible for freeing the list.
//
// Individual arguments must be one of int, uint, int32, uint32, int64, uint64,
// float64, uintptr or Intptr. Other types will panic.
//
// Note: The C translated to Go varargs ABI alignment for all types is 8 at all
// architectures.
func VaList(p uintptr, args ...interface{}) (r uintptr) {
	if p&7 != 0 {
		panic("internal error")
	}

	r = p
	for _, v := range args {
		switch x := v.(type) {
		case int:
			*(*int64)(unsafe.Pointer(p)) = int64(x)
		case int32:
			*(*int64)(unsafe.Pointer(p)) = int64(x)
		case int64:
			*(*int64)(unsafe.Pointer(p)) = x
		case uint:
			*(*uint64)(unsafe.Pointer(p)) = uint64(x)
		case uint32:
			*(*uint64)(unsafe.Pointer(p)) = uint64(x)
		case uint64:
			*(*uint64)(unsafe.Pointer(p)) = x
		case float64:
			*(*float64)(unsafe.Pointer(p)) = x
		case uintptr:
			*(*uint64)(unsafe.Pointer(p)) = uint64(x)
		default:
			panic(todo("invalid VaList argument type: %T", x))
		}
		p += 8
	}
	return r
}

func VaInt32(app *uintptr) int32 {
	ap := *(*uintptr)(unsafe.Pointer(app))
	ap = roundup(ap, 8)
	v := *(*int32)(unsafe.Pointer(ap))
	ap += 8
	*(*uintptr)(unsafe.Pointer(app)) = ap
	return v
}

func VaUint32(app *uintptr) uint32 {
	ap := *(*uintptr)(unsafe.Pointer(app))
	ap = roundup(ap, 8)
	v := *(*uint32)(unsafe.Pointer(ap))
	ap += 8
	*(*uintptr)(unsafe.Pointer(app)) = ap
	return v
}

func VaInt64(app *uintptr) int64 {
	ap := *(*uintptr)(unsafe.Pointer(app))
	ap = roundup(ap, 8)
	v := *(*int64)(unsafe.Pointer(ap))
	ap += 8
	*(*uintptr)(unsafe.Pointer(app)) = ap
	return v
}

func VaUint64(app *uintptr) uint64 {
	ap := *(*uintptr)(unsafe.Pointer(app))
	ap = roundup(ap, 8)
	v := *(*uint64)(unsafe.Pointer(ap))
	ap += 8
	*(*uintptr)(unsafe.Pointer(app)) = ap
	return v
}

func VaFloat32(app *uintptr) float32 {
	ap := *(*uintptr)(unsafe.Pointer(app))
	ap = roundup(ap, 8)
	v := *(*float64)(unsafe.Pointer(ap))
	ap += 8
	*(*uintptr)(unsafe.Pointer(app)) = ap
	return float32(v)
}

func VaFloat64(app *uintptr) float64 {
	ap := *(*uintptr)(unsafe.Pointer(app))
	ap = roundup(ap, 8)
	v := *(*float64)(unsafe.Pointer(ap))
	ap += 8
	*(*uintptr)(unsafe.Pointer(app)) = ap
	return v
}

func VaUintptr(app *uintptr) uintptr {
	ap := *(*uintptr)(unsafe.Pointer(app))
	ap = roundup(ap, 8)
	v := *(*uintptr)(unsafe.Pointer(ap))
	ap += 8
	*(*uintptr)(unsafe.Pointer(app)) = ap
	return v
}

func roundup(n, to uintptr) uintptr {
	if r := n % to; r != 0 {
		return n + to - r
	}

	return n
}

func GoString(s uintptr) string {
	if s == 0 {
		return ""
	}

	var buf []byte
	for {
		b := *(*byte)(unsafe.Pointer(s))
		if b == 0 {
			return string(buf)
		}

		buf = append(buf, b)
		s++
	}
}

// GoBytes returns a byte slice from a C char* having length len bytes.
func GoBytes(s uintptr, len int) []byte {
	if len == 0 {
		return nil
	}

	return (*RawMem)(unsafe.Pointer(s))[:len:len]
}

func mustCalloc(t *TLS, n types.Size_t) uintptr {
	if p := Xcalloc(t, 1, n); p != 0 {
		return p
	}

	panic("OOM")
}

func Bool32(b bool) int32 {
	if b {
		return 1
	}

	return 0
}

func Bool64(b bool) int64 {
	if b {
		return 1
	}

	return 0
}

type sorter struct {
	len  int
	base uintptr
	sz   uintptr
	f    func(*TLS, uintptr, uintptr) int32
	t    *TLS
}

func (s *sorter) Len() int { return s.len }

func (s *sorter) Less(i, j int) bool {
	return s.f(s.t, s.base+uintptr(i)*s.sz, s.base+uintptr(j)*s.sz) < 0
}

func (s *sorter) Swap(i, j int) {
	p := uintptr(s.base + uintptr(i)*s.sz)
	q := uintptr(s.base + uintptr(j)*s.sz)
	for i := 0; i < int(s.sz); i++ {
		*(*byte)(unsafe.Pointer(p)), *(*byte)(unsafe.Pointer(q)) = *(*byte)(unsafe.Pointer(q)), *(*byte)(unsafe.Pointer(p))
		p++
		q++
	}
}

func CString(s string) (uintptr, error) {
	n := len(s)
	p := Xmalloc(nil, types.Size_t(n)+1)
	if p == 0 {
		return 0, fmt.Errorf("CString: cannot allocate %d bytes", n+1)
	}

	copy((*RawMem)(unsafe.Pointer(p))[:n:n], s)
	*(*byte)(unsafe.Pointer(p + uintptr(n))) = 0
	return p, nil
}

func mustCString(s string) uintptr {
	p, err := CString(s)
	if err != nil {
		panic(todo("", err))
	}
	return p
}

func GetEnviron() (r []string) {
	for p := Environ(); ; p += unsafe.Sizeof(p) {
		q := *(*uintptr)(unsafe.Pointer(p))
		if q == 0 {
			return r
		}

		r = append(r, GoString(q))
	}
}

func strToUint64(t *TLS, s uintptr, base int32) (seenDigits, neg bool, next uintptr, n uint64, err int32) {
	var c byte
out:
	for {
		c = *(*byte)(unsafe.Pointer(s))
		switch c {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			s++
		case '+':
			s++
			break out
		case '-':
			s++
			neg = true
			break out
		default:
			break out
		}
	}
	for {
		c = *(*byte)(unsafe.Pointer(s))
		var digit uint64
		switch base {
		case 10:
			switch {
			case c >= '0' && c <= '9':
				seenDigits = true
				digit = uint64(c) - '0'
			default:
				return seenDigits, neg, s, n, 0
			}
		case 16:
			if c >= 'A' && c <= 'F' {
				c = c + ('a' - 'A')
			}
			switch {
			case c >= '0' && c <= '9':
				seenDigits = true
				digit = uint64(c) - '0'
			case c >= 'a' && c <= 'f':
				seenDigits = true
				digit = uint64(c) - 'a' + 10
			default:
				return seenDigits, neg, s, n, 0
			}
		default:
			panic(todo("", base))
		}
		n0 := n
		n = uint64(base)*n + digit
		if n < n0 { // overflow
			return seenDigits, neg, s, n0, errno.ERANGE
		}

		s++
	}
}

func strToFloatt64(t *TLS, s uintptr, bits int) (n float64, errno int32) {
	var c byte
out:
	for {
		c = *(*byte)(unsafe.Pointer(s))
		switch c {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			s++
		case '+':
			s++
			break out
		case '-':
			s++
			break out
		default:
			break out
		}
	}
	var b []byte
	for {
		c = *(*byte)(unsafe.Pointer(s))
		switch {
		case c >= '0' && c <= '9':
			b = append(b, c)
		case c == '.':
			b = append(b, c)
			s++
			for {
				c = *(*byte)(unsafe.Pointer(s))
				switch {
				case c >= '0' && c <= '9':
					b = append(b, c)
				case c == 'e' || c == 'E':
					b = append(b, c)
					s++
					for {
						c = *(*byte)(unsafe.Pointer(s))
						switch {
						case c == '+' || c == '-':
							b = append(b, c)
							s++
							for {
								c = *(*byte)(unsafe.Pointer(s))
								switch {
								case c >= '0' && c <= '9':
									b = append(b, c)
								default:
									var err error
									n, err = strconv.ParseFloat(string(b), bits)
									if err != nil {
										panic(todo(""))
									}

									return n, 0
								}

								s++
							}
						default:
							panic(todo("%q %q", b, string(c)))
						}

						s++
					}
				default:
					panic(todo("%q %q", b, string(c)))
				}

				s++
			}
		default:
			panic(todo("%q %q", b, string(c)))
		}

		s++
	}
}

func parseZone(s string) (name string, off int) {
	_, name, off, _ = parseZoneOffset(s, false)
	return name, off
}

func parseZoneOffset(s string, offOpt bool) (string, string, int, bool) {
	name := s
	for len(s) != 0 {
		switch c := s[0]; {
		case c >= 'A' && c <= 'Z':
			s = s[1:]
		default:
			name = name[:len(name)-len(s)]
			if len(name) < 3 {
				panic(todo(""))
			}

			if offOpt {
				if len(s) == 0 {
					return "", name, 0, false
				}

				if c := s[0]; (c < '0' || c > '9') && c != '+' && c != '-' {
					return s, name, 0, false
				}
			}

			s, off := parseOffset(s)
			return s, name, off, true
		}
	}
	panic(todo(""))
}

//  [+|-]hh[:mm[:ss]]
func parseOffset(s string) (string, int) {
	if len(s) == 0 {
		panic(todo(""))
	}

	k := 1
	switch s[0] {
	case '+':
		// nop
		s = s[1:]
	case '-':
		k = -1
		s = s[1:]
	}
	s, hh, ok := parseUint(s)
	if !ok {
		panic(todo(""))
	}

	n := hh * 3600
	if len(s) == 0 || s[0] != ':' {
		return s, k * n
	}

	s = s[1:] // ':'
	if len(s) == 0 {
		panic(todo(""))
	}

	s, mm, ok := parseUint(s)
	if !ok {
		panic(todo(""))
	}

	n += mm * 60
	if len(s) == 0 || s[0] != ':' {
		return s, k * n
	}

	s = s[1:] // ':'
	if len(s) == 0 {
		panic(todo(""))
	}

	s, ss, _ := parseUint(s)
	return s, k * (n + ss)
}

func parseUint(s string) (string, int, bool) {
	var ok bool
	var r int
	for len(s) != 0 {
		switch c := s[0]; {
		case c >= '0' && c <= '9':
			ok = true
			r0 := r
			r = 10*r + int(c) - '0'
			if r < r0 {
				panic(todo(""))
			}

			s = s[1:]
		default:
			return s, r, ok
		}
	}
	return s, r, ok
}

// https://stackoverflow.com/a/53052382
//
// isTimeDST returns true if time t occurs within daylight saving time
// for its time zone.
func isTimeDST(t time.Time) bool {
	// If the most recent (within the last year) clock change
	// was forward then assume the change was from std to dst.
	hh, mm, _ := t.UTC().Clock()
	tClock := hh*60 + mm
	for m := -1; m > -12; m-- {
		// assume dst lasts for at least one month
		hh, mm, _ := t.AddDate(0, m, 0).UTC().Clock()
		clock := hh*60 + mm
		if clock != tClock {
			return clock > tClock
		}
	}
	// assume no dst
	return false
}
