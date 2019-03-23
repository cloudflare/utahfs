// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package readahead provides readers which enable concurrent read from seekable or compressed files.
// It's useful when reading from a network file system (like Google Cloud Storage).
package readahead // import "github.com/google/readahead"

import (
	"errors"
	"io"
	"sync"

	"github.com/golang/glog"
)

// reader takes a ReaderAt and reads ahead concurrently, while providing io.Reader functionality.
type reader struct {
	name        string
	err         error
	buf         []byte
	bufToReturn []byte
	chunkPool   sync.Pool
	chunkRespc  <-chan *chunkResp
	closedc     chan<- struct{}
	done        sync.WaitGroup
}

func makeReader(name string, chunkSize int, chunkAhead int) (chan<- *chunkResp, <-chan struct{}, *reader) {
	chunkRespc := make(chan *chunkResp, chunkAhead)
	closedc := make(chan struct{})
	res := &reader{name: name, chunkRespc: chunkRespc, closedc: closedc}
	res.chunkPool.New = func() interface{} {
		return make([]byte, chunkSize)
	}
	res.done.Add(1)
	return chunkRespc, closedc, res
}

// NewConcurrentReader creates a new reader with the specified chunk size and number of workers.
// Name is only used for logging. It reads ahead up to chunkAhead chunks of chunkSize with numWorkers
// and tries to maintain the readahead buffer.
func NewConcurrentReader(name string, r io.ReaderAt, chunkSize int, chunkAhead int, numWorkers int) io.ReadCloser {
	chunkRespc, closedc, res := makeReader(name, chunkSize, chunkAhead)
	go func() {
		defer res.done.Done()
		runAt(name, r, chunkRespc, closedc, chunkSize, chunkAhead, numWorkers, &res.chunkPool)
	}()
	return res
}

// NewReader creates a readahead reader. It will read up to chunkAhead chunks of
// chunkSize bytes each and use a separate goroutine for that. It is useful when reading
// from a compressed stream or from network. If an incoming stream supports io.ReaderAt,
// NewConcurrentReader is a faster option. Name is only used for logging.
// The resulting reader must be read to EOF or Close must be called to
// prevent memory leaks.
func NewReader(name string, r io.Reader, chunkSize, chunkAhead int) io.ReadCloser {
	chunkRespc, closedc, res := makeReader(name, chunkSize, chunkAhead)
	go func() {
		defer res.done.Done()
		run(name, r, chunkRespc, closedc, &res.chunkPool)
	}()
	return res
}

type chunkResp struct {
	off   int64
	err   error
	chunk []byte
}

func run(name string, in io.Reader, chunkRespc chan<- *chunkResp, closedc <-chan struct{}, chunkPool *sync.Pool) {
	defer close(chunkRespc)

	var off int64
	for {
		chunk := chunkPool.Get().([]byte)
		n, err := in.Read(chunk)
		glog.V(2).Infof("Read %d bytes, got error %v", n, err)
		chunk = chunk[:n]
		if err != nil && err != io.EOF {
			glog.Errorf("readahead %s: about to report an error %v", name, err)
		}
		select {
		case chunkRespc <- &chunkResp{chunk: chunk, off: off, err: err}:
		case <-closedc:
			return
		}
		if err != nil {
			return
		}
		off += int64(len(chunk))
	}
}

func runAt(name string, in io.ReaderAt, chunkRespc chan<- *chunkResp, closedc <-chan struct{}, chunkSize, chunkAhead, numWorkers int, chunkPool *sync.Pool) {
	// runAt sets up a pipeline of Goroutines:
	// - A goroutine to write read offsets to reqCh
	// - numWorkers goroutines to perform reads in parallel, sending results to respCh
	// - A goroutine to close respCh when the worker goroutines finish
	// - This goroutine reads from respCh until it is closed,
	//   closing eofCh to halt the first goroutine on the first error.
	defer close(chunkRespc)

	reqCh := make(chan int64, numWorkers)
	respCh := make(chan *chunkResp, numWorkers)
	eofCh := make(chan struct{}) // eofCh is closed when we reach EOF or we encounter an error

	// Write read offsets to reqCh until eofCh or closedc are closed.
	go func() {
		defer close(reqCh)
		var off int64
		for {
			glog.V(2).Infof("Attempting to read %d", off)
			select {
			case reqCh <- off:
			case <-eofCh:
				glog.V(2).Info("eofCh closed, closing reqCh")
				return
			case <-closedc:
				glog.V(2).Info("closedc closed, closing reqCh")
				return
			}
			off += int64(chunkSize)
		}
	}()

	// Worker goroutines that read from reqCh and write to respCh until reqCh is closed.
	var readers sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			chunkReader(in, reqCh, chunkPool, respCh, closedc)
		}()
	}
	// Close respCh when the worker goroutines are done.
	go func() {
		readers.Wait()
		glog.V(2).Info("Readers finished, closing read response channel")
		close(respCh)
	}()
	// Read from respCh until it or closedc is closed.
	reorderChunks(name, respCh, chunkRespc, eofCh, closedc)

	// Wait for any remaining chunkReader goroutines to exit to avoid racing
	// between ReadAt and Close.
	readers.Wait()
}

// reorderChunks consumes out-of-order chunks from respCh and emits
// them in order to chunkRespc. When a chunk is received with an
// error, it closes eofCh to prevent new chunks from being read.
func reorderChunks(name string, respCh <-chan *chunkResp, chunkRespc chan<- *chunkResp, eofCh chan<- struct{}, closedc <-chan struct{}) {
	pending := make(map[int64]*chunkResp)
	var off int64
	for resp := range respCh {
		glog.V(2).Infof("Received chunk (off=%d chunk=<%d bytes> err=%v", resp.off, len(resp.chunk), resp.err)
		pending[resp.off] = resp
		if resp.err != nil && eofCh != nil {
			close(eofCh)
			eofCh = nil
		}
		for {
			resp, ok := pending[off]
			if !ok {
				break
			}
			if resp.err != nil && resp.err != io.EOF {
				glog.Errorf("readahead %s: about to report an error %v", name, resp.err)
			}
			glog.V(2).Infof("Returning chunk (off=%d chunk=<%d bytes> err=%v", resp.off, len(resp.chunk), resp.err)
			select {
			case chunkRespc <- resp:
			case <-closedc:
				// N.B. We are aborting here before draining respCh; the other
				// goroutines will abort asynchronously on closedc as well.
				return
			}
			delete(pending, off)
			off += int64(len(resp.chunk))
		}
	}
}

func chunkReader(in io.ReaderAt, offs <-chan int64, chunkPool *sync.Pool, res chan<- *chunkResp, closedc <-chan struct{}) {
	for off := range offs {
		chunk := chunkPool.Get().([]byte)
		n, err := in.ReadAt(chunk, off)
		if n < len(chunk) && err == nil {
			glog.Errorf("ReaderAt semantics violation: len(chunk): %d, nread: %d, err: %v", len(chunk), n, err)
		}
		if err != nil {
			select {
			case res <- &chunkResp{off: off, chunk: chunk[:n], err: err}:
			case <-closedc:
				return
			}
			continue
		}
		select {
		case res <- &chunkResp{off: off, chunk: chunk}:
		case <-closedc:
			return
		}
	}
}

// Read implements io.Reader.
func (r *reader) Read(b []byte) (int, error) {
	if len(r.buf) == 0 {
		if r.bufToReturn != nil {
			r.chunkPool.Put(r.bufToReturn)
			r.bufToReturn = nil
		}
		if r.err != nil {
			// Make sure any goroutines are done. Ignore
			// the error since it will just be r.err.
			r.Close()
			return 0, r.err
		}
		resp, ok := <-r.chunkRespc
		if !ok {
			glog.Fatal("chunkRespc channel has been closed without a final error")
		}
		if resp.err != nil {
			r.err = resp.err
		}
		r.buf = resp.chunk
		r.bufToReturn = resp.chunk[:cap(resp.chunk)]
	}
	n := copy(b, r.buf)
	r.buf = r.buf[n:]
	if len(r.buf) == 0 {
		return n, r.err
	}
	return n, nil
}

func (r *reader) Close() error {
	if r.closedc != nil {
		close(r.closedc)
		r.closedc = nil
	}

	r.done.Wait()

	err := r.err
	if err == nil {
		r.err = errors.New("readahead reader already closed")
		r.buf = nil
	}
	return err
}
