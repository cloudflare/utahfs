// Copyright 2020 The Libc Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go.generate echo package libc > ccgo.go
//go:generate go run generate.go
//go:generate go fmt ./...

// Package libc provides run time support for ccgo generated programs and
// implements selected parts of the C standard library.
package libc // import "modernc.org/libc"

//TODO use O_RDONLY etc. from fcntl header

//TODO use t.Alloc/Free where appropriate

import (
	"fmt"
	"math"
	"os"
	gosignal "os/signal"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	gotime "time"
	"unsafe"

	"github.com/mattn/go-isatty"
	"modernc.org/libc/errno"
	"modernc.org/libc/signal"
	"modernc.org/libc/stdio"
	"modernc.org/libc/sys/types"
	"modernc.org/libc/time"
	"modernc.org/libc/unistd"
	"modernc.org/memory"
)

var (
	allocMu   sync.Mutex
	allocator memory.Allocator
)

// Keep these outside of the var block otherwise go generate will miss them.
var Xenviron uintptr
var Xstdin = newFile(nil, unistd.STDIN_FILENO)
var Xstdout = newFile(nil, unistd.STDOUT_FILENO)
var Xstderr = newFile(nil, unistd.STDERR_FILENO)

func Environ() uintptr {
	return Xenviron
}

func EnvironP() uintptr {
	return uintptr(unsafe.Pointer(&Xenviron))
}

func X___errno_location(t *TLS) uintptr {
	return X__errno_location(t)
}

// int * __errno_location(void);
func X__errno_location(t *TLS) uintptr {
	return t.errnop
}

func Start(main func(*TLS, int32, uintptr) int32) {
	runtime.LockOSThread()
	t := NewTLS()
	argv := mustCalloc(t, types.Size_t((len(os.Args)+1)*int(uintptrSize)))
	p := argv
	for _, v := range os.Args {
		s := mustCalloc(t, types.Size_t(len(v)+1))
		copy((*RawMem)(unsafe.Pointer(s))[:len(v):len(v)], v)
		*(*uintptr)(unsafe.Pointer(p)) = s
		p += uintptrSize
	}
	SetEnviron(t, os.Environ())
	Xexit(t, main(t, int32(len(os.Args)), argv))
}

func SetEnviron(t *TLS, env []string) {
	p := mustCalloc(t, types.Size_t((len(env)+1)*(int(uintptrSize))))
	*(*uintptr)(unsafe.Pointer(EnvironP())) = p
	for _, v := range env {
		s := mustCalloc(t, types.Size_t(len(v)+1))
		copy((*(*RawMem)(unsafe.Pointer(s)))[:len(v):len(v)], v)
		*(*uintptr)(unsafe.Pointer(p)) = s
		p += uintptrSize
	}
}

// void *malloc(size_t size);
func Xmalloc(t *TLS, n types.Size_t) uintptr {
	if n == 0 {
		return 0
	}

	allocMu.Lock()

	defer allocMu.Unlock()

	p, err := allocator.UintptrMalloc(int(n))
	if err != nil {
		t.setErrno(errno.ENOMEM)
		return 0
	}

	return p
}

// void *calloc(size_t nmemb, size_t size);
func Xcalloc(t *TLS, n, size types.Size_t) uintptr {
	rq := int(n * size)
	if rq == 0 {
		return 0
	}

	allocMu.Lock()

	defer allocMu.Unlock()

	p, err := allocator.UintptrCalloc(int(n * size))
	if err != nil {
		t.setErrno(errno.ENOMEM)
		return 0
	}

	return p
}

// void *realloc(void *ptr, size_t size);
func Xrealloc(t *TLS, ptr uintptr, size types.Size_t) uintptr {
	allocMu.Lock()

	defer allocMu.Unlock()

	p, err := allocator.UintptrRealloc(ptr, int(size))
	if err != nil {
		t.setErrno(errno.ENOMEM)
		return 0
	}

	return p
}

// void free(void *ptr);
func Xfree(t *TLS, p uintptr) {
	if p == 0 {
		return
	}

	allocMu.Lock()

	defer allocMu.Unlock()

	allocator.UintptrFree(p)
}

func write(b []byte) (int, error) {
	if dmesgs {
		dmesg("%v: %s", origin(1), b)
	}
	if _, err := os.Stdout.Write(b); err != nil {
		return -1, err
	}

	return len(b), nil
}

func X__builtin_abort(t *TLS)                                        { Xabort(t) }
func X__builtin_abs(t *TLS, j int32) int32                           { return Xabs(t, j) }
func X__builtin_copysign(t *TLS, x, y float64) float64               { return Xcopysign(t, x, y) }
func X__builtin_copysignf(t *TLS, x, y float32) float32              { return Xcopysignf(t, x, y) }
func X__builtin_exit(t *TLS, status int32)                           { Xexit(t, status) }
func X__builtin_expect(t *TLS, exp, c long) long                     { return exp }
func X__builtin_fabs(t *TLS, x float64) float64                      { return Xfabs(t, x) }
func X__builtin_free(t *TLS, ptr uintptr)                            { Xfree(t, ptr) }
func X__builtin_malloc(t *TLS, size types.Size_t) uintptr            { return Xmalloc(t, size) }
func X__builtin_memcmp(t *TLS, s1, s2 uintptr, n types.Size_t) int32 { return Xmemcmp(t, s1, s2, n) }
func X__builtin_prefetch(t *TLS, addr, args uintptr)                 {}
func X__builtin_printf(t *TLS, s, args uintptr) int32                { return Xprintf(t, s, args) }
func X__builtin_strchr(t *TLS, s uintptr, c int32) uintptr           { return Xstrchr(t, s, c) }
func X__builtin_strcmp(t *TLS, s1, s2 uintptr) int32                 { return Xstrcmp(t, s1, s2) }
func X__builtin_strcpy(t *TLS, dest, src uintptr) uintptr            { return Xstrcpy(t, dest, src) }
func X__builtin_strlen(t *TLS, s uintptr) types.Size_t               { return Xstrlen(t, s) }
func X__builtin_trap(t *TLS)                                         { Xabort(t) }
func X__isnan(t *TLS, arg float64) int32                             { return Xisnan(t, arg) }
func X__isnanf(t *TLS, arg float32) int32                            { return Xisnanf(t, arg) }
func X__isnanl(t *TLS, arg float64) int32                            { return Xisnanl(t, arg) }
func Xvfprintf(t *TLS, stream, format, ap uintptr) int32             { return Xfprintf(t, stream, format, ap) }

func X__builtin_unreachable(t *TLS) {
	fmt.Fprintf(os.Stderr, "unrechable\n")
	os.Stderr.Sync()
	Xexit(t, 1)
}

func X__builtin_snprintf(t *TLS, str uintptr, size types.Size_t, format, args uintptr) int32 {
	return Xsnprintf(t, str, size, format, args)
}

func X__builtin_sprintf(t *TLS, str, format, args uintptr) (r int32) {
	return Xsprintf(t, str, format, args)
}

func X__builtin_memcpy(t *TLS, dest, src uintptr, n types.Size_t) (r uintptr) {
	return Xmemcpy(t, dest, src, n)
}

func X__builtin_memset(t *TLS, s uintptr, c int32, n types.Size_t) uintptr {
	return Xmemset(t, s, c, n)
}

// int sprintf(char *str, const char *format, ...);
func Xsprintf(t *TLS, str, format, args uintptr) (r int32) {
	b := printf(format, args)
	r = int32(len(b))
	copy((*RawMem)(unsafe.Pointer(str))[:r:r], b)
	*(*byte)(unsafe.Pointer(str + uintptr(r))) = 0
	return int32(len(b))
}

// void qsort(void *base, size_t nmemb, size_t size, int (*compar)(const void *, const void *));
func Xqsort(t *TLS, base uintptr, nmemb, size types.Size_t, compar uintptr) {
	sort.Sort(&sorter{
		len:  int(nmemb),
		base: base,
		sz:   uintptr(size),
		f: (*struct {
			f func(*TLS, uintptr, uintptr) int32
		})(unsafe.Pointer(&struct{ uintptr }{compar})).f,
		t: t,
	})
}

// void __assert_fail(const char * assertion, const char * file, unsigned int line, const char * function);
func X__assert_fail(t *TLS, assertion, file uintptr, line uint32, function uintptr) {
	fmt.Fprintf(os.Stderr, "assertion failure: %s:%d.%s: %s\n", GoString(file), line, GoString(function), GoString(assertion))
	os.Stderr.Sync()
	Xexit(t, 1)
}

// int vprintf(const char *format, va_list ap);
func Xvprintf(t *TLS, s, ap uintptr) int32 { return Xprintf(t, s, ap) }

// int __isoc99_sscanf(const char *str, const char *format, ...);
func X__isoc99_sscanf(t *TLS, str, format, va uintptr) int32 {
	return scanf(strings.NewReader(GoString(str)), format, va)

}

// unsigned int sleep(unsigned int seconds);
func Xsleep(t *TLS, seconds uint32) uint32 {
	gotime.Sleep(gotime.Second * gotime.Duration(seconds))
	return 0
}

// int usleep(useconds_t usec);
func Xusleep(t *TLS, usec types.X__useconds_t) int32 {
	gotime.Sleep(gotime.Microsecond * gotime.Duration(usec))
	return 0
}

// sighandler_t signal(int signum, sighandler_t handler);
func Xsignal(t *TLS, signum int32, handler uintptr) uintptr { //TODO use sigaction?
	signalsMu.Lock()

	defer signalsMu.Unlock()

	r := signals[signum]
	signals[signum] = handler
	switch handler {
	case signal.SIG_DFL:
		panic(todo("%v %#x", syscall.Signal(signum), handler))
	case signal.SIG_IGN:
		switch r {
		case signal.SIG_DFL:
			gosignal.Ignore(syscall.Signal(signum))
		case signal.SIG_IGN:
			panic(todo("%v %#x", syscall.Signal(signum), handler))
		default:
			panic(todo("%v %#x", syscall.Signal(signum), handler))
		}
	default:
		switch r {
		case signal.SIG_DFL:
			c := make(chan os.Signal, 1)
			gosignal.Notify(c, syscall.Signal(signum))
			go func() { //TODO mechanism to stop/cancel
				for {
					<-c
					var f func(*TLS, int32)
					*(*uintptr)(unsafe.Pointer(&f)) = handler
					tls := NewTLS()
					f(tls, signum)
					tls.Close()
				}
			}()
		case signal.SIG_IGN:
			panic(todo("%v %#x", syscall.Signal(signum), handler))
		default:
			panic(todo("%v %#x", syscall.Signal(signum), handler))
		}
	}
	return r
}

// size_t strcspn(const char *s, const char *reject);
func Xstrcspn(t *TLS, s, reject uintptr) (r types.Size_t) {
	bits := newBits(256)
	for {
		c := *(*byte)(unsafe.Pointer(reject))
		if c == 0 {
			break
		}

		reject++
		bits.set(int(c))
	}
	for {
		c := *(*byte)(unsafe.Pointer(s))
		if c == 0 || bits.has(int(c)) {
			return r
		}

		s++
		r++
	}
}

// int printf(const char *format, ...);
func Xprintf(t *TLS, format, args uintptr) int32 {
	n, _ := write(printf(format, args))
	return int32(n)
}

// int fprintf(FILE *stream, const char *format, ...);
func Xfprintf(t *TLS, stream, format, args uintptr) int32 {
	n, _ := fwrite((*stdio.FILE)(unsafe.Pointer(stream)).F_fileno, printf(format, args))
	return int32(n)
}

// int snprintf(char *str, size_t size, const char *format, ...);
func Xsnprintf(t *TLS, str uintptr, size types.Size_t, format, args uintptr) (r int32) {
	switch size {
	case 0:
		return 0
	case 1:
		*(*byte)(unsafe.Pointer(str)) = 0
		return 0
	}

	b := printf(format, args)
	if len(b)+1 > int(size) {
		b = b[:size-1]
	}
	r = int32(len(b))
	copy((*RawMem)(unsafe.Pointer(str))[:r:r], b)
	*(*byte)(unsafe.Pointer(str + uintptr(r))) = 0
	return r
}

// int abs(int j);
func Xabs(t *TLS, j int32) int32 {
	if j >= 0 {
		return j
	}

	return -j
}

func Xacos(t *TLS, x float64) float64             { return math.Acos(x) }
func Xasin(t *TLS, x float64) float64             { return math.Asin(x) }
func Xatan(t *TLS, x float64) float64             { return math.Atan(x) }
func Xatan2(t *TLS, x, y float64) float64         { return math.Atan2(x, y) }
func Xceil(t *TLS, x float64) float64             { return math.Ceil(x) }
func Xcopysign(t *TLS, x, y float64) float64      { return math.Copysign(x, y) }
func Xcopysignf(t *TLS, x, y float32) float32     { return float32(math.Copysign(float64(x), float64(y))) }
func Xcos(t *TLS, x float64) float64              { return math.Cos(x) }
func Xcosf(t *TLS, x float32) float32             { return float32(math.Cos(float64(x))) }
func Xcosh(t *TLS, x float64) float64             { return math.Cosh(x) }
func Xexp(t *TLS, x float64) float64              { return math.Exp(x) }
func Xfabs(t *TLS, x float64) float64             { return math.Abs(x) }
func Xfabsf(t *TLS, x float32) float32            { return float32(math.Abs(float64(x))) }
func Xfloor(t *TLS, x float64) float64            { return math.Floor(x) }
func Xfmod(t *TLS, x, y float64) float64          { return math.Mod(x, y) }
func Xhypot(t *TLS, x, y float64) float64         { return math.Hypot(x, y) }
func Xisnan(t *TLS, x float64) int32              { return Bool32(math.IsNaN(x)) }
func Xisnanf(t *TLS, x float32) int32             { return Bool32(math.IsNaN(float64(x))) }
func Xisnanl(t *TLS, x float64) int32             { return Bool32(math.IsNaN(x)) } // ccgo has to handle long double as double as Go does not support long double.
func Xldexp(t *TLS, x float64, exp int32) float64 { return math.Ldexp(x, int(exp)) }
func Xlog(t *TLS, x float64) float64              { return math.Log(x) }
func Xlog10(t *TLS, x float64) float64            { return math.Log10(x) }
func Xround(t *TLS, x float64) float64            { return math.Round(x) }
func Xsin(t *TLS, x float64) float64              { return math.Sin(x) }
func Xsinf(t *TLS, x float32) float32             { return float32(math.Sin(float64(x))) }
func Xsinh(t *TLS, x float64) float64             { return math.Sinh(x) }
func Xsqrt(t *TLS, x float64) float64             { return math.Sqrt(x) }
func Xtan(t *TLS, x float64) float64              { return math.Tan(x) }
func Xtanh(t *TLS, x float64) float64             { return math.Tanh(x) }

var nextRand = uint64(1)

// int rand(void);
func Xrand(t *TLS) int32 {
	nextRand = nextRand*1103515245 + 12345
	return int32(uint32(nextRand / (math.MaxUint32 + 1) % math.MaxInt32))
}

func Xpow(t *TLS, x, y float64) float64 {
	r := math.Pow(x, y)
	if x > 0 && r == 1 && y >= -1.0000000000000000715e-18 && y < -1e-30 {
		r = 0.9999999999999999
	}
	return r
}

func Xfrexp(t *TLS, x float64, exp uintptr) float64 {
	f, e := math.Frexp(x)
	*(*int32)(unsafe.Pointer(exp)) = int32(e)
	return f
}

func Xmodf(t *TLS, x float64, iptr uintptr) float64 {
	i, f := math.Modf(x)
	*(*float64)(unsafe.Pointer(iptr)) = i
	return f
}

// char *strncpy(char *dest, const char *src, size_t n)
func Xstrncpy(t *TLS, dest, src uintptr, n types.Size_t) (r uintptr) {
	r = dest
	for c := *(*int8)(unsafe.Pointer(src)); c != 0 && n > 0; n-- {
		*(*int8)(unsafe.Pointer(dest)) = c
		dest++
		src++
		c = *(*int8)(unsafe.Pointer(src))
	}
	for ; uintptr(n) > 0; n-- {
		*(*int8)(unsafe.Pointer(dest)) = 0
		dest++
	}
	return r
}

// int strcmp(const char *s1, const char *s2)
func Xstrcmp(t *TLS, s1, s2 uintptr) int32 {
	for {
		ch1 := *(*byte)(unsafe.Pointer(s1))
		s1++
		ch2 := *(*byte)(unsafe.Pointer(s2))
		s2++
		if ch1 != ch2 || ch1 == 0 || ch2 == 0 {
			return int32(ch1) - int32(ch2)
		}
	}
}

// size_t strlen(const char *s)
func Xstrlen(t *TLS, s uintptr) (r types.Size_t) {
	for ; *(*int8)(unsafe.Pointer(s)) != 0; s++ {
		r++
	}
	return r
}

// char *strcat(char *dest, const char *src)
func Xstrcat(t *TLS, dest, src uintptr) (r uintptr) {
	r = dest
	for *(*int8)(unsafe.Pointer(dest)) != 0 {
		dest++
	}
	for {
		c := *(*int8)(unsafe.Pointer(src))
		src++
		*(*int8)(unsafe.Pointer(dest)) = c
		dest++
		if c == 0 {
			return r
		}
	}
}

// int strncmp(const char *s1, const char *s2, size_t n)
func Xstrncmp(t *TLS, s1, s2 uintptr, n types.Size_t) int32 {
	var ch1, ch2 byte
	for ; n != 0; n-- {
		ch1 = *(*byte)(unsafe.Pointer(s1))
		s1++
		ch2 = *(*byte)(unsafe.Pointer(s2))
		s2++
		if ch1 != ch2 {
			return int32(ch1) - int32(ch2)
		}

		if ch1 == 0 {
			return 0
		}
	}
	return 0
}

// char *strcpy(char *dest, const char *src)
func Xstrcpy(t *TLS, dest, src uintptr) (r uintptr) {
	r = dest
	// src0 := src
	for ; ; dest++ {
		c := *(*int8)(unsafe.Pointer(src))
		src++
		*(*int8)(unsafe.Pointer(dest)) = c
		if c == 0 {
			return r
		}
	}
}

// char *strchr(const char *s, int c)
func Xstrchr(t *TLS, s uintptr, c int32) uintptr {
	for {
		ch2 := *(*byte)(unsafe.Pointer(s))
		if ch2 == byte(c) {
			return s
		}

		if ch2 == 0 {
			return 0
		}

		s++
	}
}

// char *strrchr(const char *s, int c)
func Xstrrchr(t *TLS, s uintptr, c int32) (r uintptr) {
	for {
		ch2 := *(*byte)(unsafe.Pointer(s))
		if ch2 == 0 {
			return r
		}

		if ch2 == byte(c) {
			r = s
		}
		s++
	}
}

// void *memset(void *s, int c, size_t n)
func Xmemset(t *TLS, s uintptr, c int32, n types.Size_t) uintptr {
	if n != 0 {
		b := (*RawMem)(unsafe.Pointer(s))[:n:n]
		for i := range b {
			b[i] = byte(c)
		}
	}
	return s
}

// void *memcpy(void *dest, const void *src, size_t n);
func Xmemcpy(t *TLS, dest, src uintptr, n types.Size_t) (r uintptr) {
	r = dest
	for ; n != 0; n-- {
		*(*byte)(unsafe.Pointer(dest)) = *(*byte)(unsafe.Pointer(src))
		src++
		dest++
	}
	return r
}

// int memcmp(const void *s1, const void *s2, size_t n);
func Xmemcmp(t *TLS, s1, s2 uintptr, n types.Size_t) int32 {
	for ; n != 0; n-- {
		c1 := *(*byte)(unsafe.Pointer(s1))
		s1++
		c2 := *(*byte)(unsafe.Pointer(s2))
		s2++
		if c1 < c2 {
			return -1
		}

		if c1 > c2 {
			return 1
		}
	}
	return 0
}

// void *memchr(const void *s, int c, size_t n);
func Xmemchr(t *TLS, s uintptr, c int32, n types.Size_t) uintptr {
	for ; n != 0; n-- {
		if *(*byte)(unsafe.Pointer(s)) == byte(c) {
			return s
		}

		s++
	}
	return 0
}

// void rewind(FILE *stream);
func Xrewind(t *TLS, stream uintptr) {
	Xfseek(t, stream, 0, stdio.SEEK_SET)
}

// void *memmove(void *dest, const void *src, size_t n);
func Xmemmove(t *TLS, dest, src uintptr, n types.Size_t) uintptr {
	copy((*RawMem)(unsafe.Pointer(uintptr(dest)))[:n:n], (*RawMem)(unsafe.Pointer(uintptr(src)))[:n:n])
	return dest
}

var getenvOnce sync.Once

// char *getenv(const char *name);
func Xgetenv(t *TLS, name uintptr) uintptr {
	p := Environ()
	if p == 0 {
		getenvOnce.Do(func() {
			SetEnviron(t, os.Environ())
			p = Environ()
		})
	}

	return getenv(p, GoString(name))
}

func getenv(p uintptr, nm string) uintptr {
	for ; ; p += uintptrSize {
		q := *(*uintptr)(unsafe.Pointer(p))
		if q == 0 {
			return 0
		}

		s := GoString(q)
		a := strings.SplitN(s, "=", 2)
		if len(a) != 2 {
			panic(todo("%q %q %q", nm, s, a))
		}

		if a[0] == nm {
			return q + uintptr(len(nm)) + 1
		}
	}
}

// char *strstr(const char *haystack, const char *needle);
func Xstrstr(t *TLS, haystack, needle uintptr) uintptr {
	hs := GoString(haystack)
	nd := GoString(needle)
	if i := strings.Index(hs, nd); i >= 0 {
		r := haystack + uintptr(i)
		return r
	}

	return 0
}

// int putc(int c, FILE *stream);
func Xputc(t *TLS, c int32, fp uintptr) int32 {
	return Xfputc(t, c, fp)
}

// int atoi(const char *nptr);
func Xatoi(t *TLS, nptr uintptr) int32 {
	_, neg, _, n, _ := strToUint64(t, nptr, 10)
	switch {
	case neg:
		return int32(-n)
	default:
		return int32(n)
	}
}

// double atof(const char *nptr);
func Xatof(t *TLS, nptr uintptr) float64 {
	n, _ := strToFloatt64(t, nptr, 64)
	return n
}

// int tolower(int c);
func Xtolower(t *TLS, c int32) int32 {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}

	return c
}

// int toupper(int c);
func Xtoupper(t *TLS, c int32) int32 {
	if c >= 'a' && c <= 'z' {
		return c - ('a' - 'A')
	}

	return c
}

// int isatty(int fd);
func Xisatty(t *TLS, fd int32) int32 {
	return Bool32(isatty.IsTerminal(uintptr(fd)))
}

// char *strdup(const char *s);
func Xstrdup(t *TLS, s uintptr) uintptr {
	panic(todo(""))
}

// long atol(const char *nptr);
func Xatol(t *TLS, nptr uintptr) long {
	_, neg, _, n, _ := strToUint64(t, nptr, 10)
	switch {
	case neg:
		return long(-n)
	default:
		return long(n)
	}
}

// int putchar(int c);
func Xputchar(t *TLS, c int32) int32 {
	if _, err := write([]byte{byte(c)}); err != nil {
		return stdio.EOF
	}

	return int32(c)
}

// time_t mktime(struct tm *tm);
func Xmktime(t *TLS, ptm uintptr) types.Time_t {
	loc := gotime.Local
	if r := getenv(Environ(), "TZ"); r != 0 {
		zone, off := parseZone(GoString(r))
		loc = gotime.FixedZone(zone, off)
	}
	tt := gotime.Date(
		int((*time.Tm)(unsafe.Pointer(ptm)).Ftm_year+1900),
		gotime.Month((*time.Tm)(unsafe.Pointer(ptm)).Ftm_mon+1),
		int((*time.Tm)(unsafe.Pointer(ptm)).Ftm_mday),
		int((*time.Tm)(unsafe.Pointer(ptm)).Ftm_hour),
		int((*time.Tm)(unsafe.Pointer(ptm)).Ftm_min),
		int((*time.Tm)(unsafe.Pointer(ptm)).Ftm_sec),
		0,
		loc,
	)
	(*time.Tm)(unsafe.Pointer(ptm)).Ftm_wday = int32(tt.Weekday())
	(*time.Tm)(unsafe.Pointer(ptm)).Ftm_yday = int32(tt.YearDay() - 1)
	return types.Time_t(tt.Unix())
}

// char *strpbrk(const char *s, const char *accept);
func Xstrpbrk(t *TLS, s, accept uintptr) uintptr {
	bits := newBits(256)
	for {
		b := *(*byte)(unsafe.Pointer(accept))
		if b == 0 {
			break
		}

		bits.set(int(b))
		accept++
	}
	for {
		b := *(*byte)(unsafe.Pointer(s))
		if b == 0 {
			return 0
		}

		if bits.has(int(b)) {
			return s
		}

		s++
	}
}

// int strcasecmp(const char *s1, const char *s2);
func Xstrcasecmp(t *TLS, s1, s2 uintptr) int32 {
	for {
		ch1 := *(*byte)(unsafe.Pointer(s1))
		if ch1 >= 'a' && ch1 <= 'z' {
			ch1 = ch1 - ('a' - 'A')
		}
		s1++
		ch2 := *(*byte)(unsafe.Pointer(s2))
		if ch2 >= 'a' && ch2 <= 'z' {
			ch2 = ch2 - ('a' - 'A')
		}
		s2++
		if ch1 != ch2 || ch1 == 0 || ch2 == 0 {
			r := int32(ch1) - int32(ch2)
			return r
		}
	}
}

var __ctype_b_table = [...]uint16{ //TODO use symbolic constants
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002,
	0x0002, 0x2003, 0x2002, 0x2002, 0x2002, 0x2002, 0x0002, 0x0002,
	0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002,
	0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002, 0x0002,
	0x6001, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004,
	0xc004, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004,
	0xd808, 0xd808, 0xd808, 0xd808, 0xd808, 0xd808, 0xd808, 0xd808,
	0xd808, 0xd808, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004,
	0xc004, 0xd508, 0xd508, 0xd508, 0xd508, 0xd508, 0xd508, 0xc508,
	0xc508, 0xc508, 0xc508, 0xc508, 0xc508, 0xc508, 0xc508, 0xc508,
	0xc508, 0xc508, 0xc508, 0xc508, 0xc508, 0xc508, 0xc508, 0xc508,
	0xc508, 0xc508, 0xc508, 0xc004, 0xc004, 0xc004, 0xc004, 0xc004,
	0xc004, 0xd608, 0xd608, 0xd608, 0xd608, 0xd608, 0xd608, 0xc608,
	0xc608, 0xc608, 0xc608, 0xc608, 0xc608, 0xc608, 0xc608, 0xc608,
	0xc608, 0xc608, 0xc608, 0xc608, 0xc608, 0xc608, 0xc608, 0xc608,
	0xc608, 0xc608, 0xc608, 0xc004, 0xc004, 0xc004, 0xc004, 0x0002,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

var ptable = uintptr(unsafe.Pointer(&__ctype_b_table[128]))

// const unsigned short * * __ctype_b_loc (void);
func X__ctype_b_loc(t *TLS) uintptr {
	return uintptr(unsafe.Pointer(&ptable))
}

func Xntohs(t *TLS, netshort uint16) uint16 {
	return uint16((*[2]byte)(unsafe.Pointer(&netshort))[0])<<8 | uint16((*[2]byte)(unsafe.Pointer(&netshort))[1])
}

// uint16_t htons(uint16_t hostshort);
func Xhtons(t *TLS, hostshort uint16) uint16 {
	var a [2]byte
	a[0] = byte(hostshort >> 8)
	a[1] = byte(hostshort)
	return *(*uint16)(unsafe.Pointer(&a))
}

// uint32_t htonl(uint32_t hostlong);
func Xhtonl(t *TLS, hostlong uint32) uint32 {
	var a [4]byte
	a[0] = byte(hostlong >> 24)
	a[1] = byte(hostlong >> 16)
	a[2] = byte(hostlong >> 8)
	a[3] = byte(hostlong)
	return *(*uint32)(unsafe.Pointer(&a))
}

// FILE *fopen(const char *pathname, const char *mode);
func Xfopen(t *TLS, pathname, mode uintptr) uintptr {
	return Xfopen64(t, pathname, mode) //TODO 32 bit
}

// void sqlite3_log(int iErrCode, const char *zFormat, ...);
func X__ccgo_sqlite3_log(t *TLS, iErrCode int32, zFormat uintptr, args uintptr) {
	if dmesgs {
		dmesg("%v: iErrCode: %v, msg: %s\n%s", origin(1), iErrCode, printf(zFormat, args), debug.Stack())
	}
}

// int _IO_putc(int __c, _IO_FILE *__fp);
func X_IO_putc(t *TLS, c int32, fp uintptr) int32 {
	return Xputc(t, c, fp)
}
