package persistent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func readInt(in []byte) int {
	out := 0
	for i := len(in) - 1; i >= 0; i-- {
		out = out<<8 + int(in[i])
	}
	return out
}

func writeInt(in int, out []byte) {
	for i := 0; i < len(out); i++ {
		out[i] = byte(in)
		in = in >> 8
	}
}

type transaction struct {
	Processed bool
	Id        uint64
	Data      map[string][]byte
}

func readTransaction(r io.Reader) (*transaction, int, error) {
	hdr := make([]byte, 17)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, 0, err
	} else if hdr[0] != 0 && hdr[0] != 1 {
		return nil, 0, fmt.Errorf("wal: data is malformed")
	}
	id, dataLen := readInt(hdr[1:9]), readInt(hdr[9:17])

	var (
		data map[string][]byte
		err  error
	)
	if hdr[0] == 0 {
		lim := &io.LimitedReader{r, int64(dataLen)}
		data, err = readMap(lim)
		if err == io.EOF || err == nil && lim.N != 0 {
			return nil, 0, io.ErrUnexpectedEOF
		} else if err != nil {
			return nil, 0, err
		}
	} else {
		// Advance the reader forward by `dataLen` amount without parsing any of
		// the data. It's not needed, the tx has already been processed.
		discard := make([]byte, 1024*1024)
		for n := 0; n < dataLen; {
			m := len(discard)
			if n+m > dataLen {
				m = dataLen - n
			}
			o, err := r.Read(discard[:m])
			if err != nil {
				return nil, 0, err
			}
			n += o
		}
	}

	return &transaction{
		Processed: hdr[0] == 1,
		Id:        uint64(id),
		Data:      data,
	}, 17 + dataLen, nil
}

func writeTransaction(w io.Writer, tx *transaction) error {
	buff := &bytes.Buffer{}
	if err := writeMap(buff, tx.Data); err != nil {
		return err
	}

	hdr := make([]byte, 17)
	if tx.Processed {
		hdr[0] = 1
	}
	writeInt(int(tx.Id), hdr[1:9])
	writeInt(buff.Len(), hdr[9:17])

	if _, err := w.Write(hdr); err != nil {
		return err
	} else if _, err := buff.WriteTo(w); err != nil {
		return err
	}
	return nil
}

var LocalWALSize = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "local_wal_size",
		Help: "The number of entries in the local WAL.",
	},
	[]string{"path"},
)

type localWAL struct {
	mu sync.Mutex

	base    ObjectStorage
	fh      *os.File
	loc     string
	maxSize int

	currSize    int
	startQueue1 int64
	endQueue1   int64
	startQueue2 int64
	nextTx      uint64
	txToPos     map[uint64]int64
	keyToTx     map[string]uint64

	wake chan struct{}
}

// NewLocalWAL returns a ReliableStorage implementation that achieves reliable
// writes over a base object storage provider by buffering writes in a
// Write-Ahead Log (WAL) stored at `loc`.
//
// The WAL may have at least `maxSize` buffered entries before new writes start
// blocking on old writes being flushed.
func NewLocalWAL(base ObjectStorage, loc string, maxSize int) (ReliableStorage, error) {
	if err := os.MkdirAll(path.Dir(loc), 0744); err != nil {
		return nil, err
	}
	fh, err := os.OpenFile(loc, os.O_RDWR|os.O_CREATE, 0744)
	if err != nil {
		return nil, err
	}

	// Read the WAL to build initial data structures.
	var (
		pos = int64(0)

		currSize    = 0
		startQueue1 = int64(0)
		endQueue1   = int64(0)
		startQueue2 = int64(0)
		nextTx      = uint64(0)
		txToPos     = make(map[uint64]int64)
		keyToTx     = make(map[string]uint64)

		foundQueue1 = false
		foundGap    = false
		foundQueue2 = false
	)

	for {
		t, n, err := readTransaction(fh)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if !t.Processed {
			currSize += len(t.Data)
		}

		if !t.Processed && !foundQueue1 {
			startQueue1 = pos
			foundQueue1 = true
		} else if t.Processed && foundQueue1 && !foundGap {
			endQueue1 = pos
			foundGap = true
		} else if !t.Processed && foundGap && !foundQueue2 {
			startQueue2 = pos
			foundQueue2 = true
		}

		if t.Id >= nextTx {
			nextTx = t.Id + 1
		}
		if !t.Processed {
			txToPos[t.Id] = pos
		}
		for key, _ := range t.Data {
			prev, ok := keyToTx[key]
			if !ok || prev < t.Id {
				keyToTx[key] = t.Id
			}
		}

		pos += int64(n)
	}

	if !foundGap {
		endQueue1 = pos
	}
	if !foundQueue2 {
		startQueue2 = pos
	}

	if currSize == 0 {
		if err := fh.Truncate(0); err != nil {
			return nil, err
		}
		startQueue1 = 0
		endQueue1 = 0
		startQueue2 = 0
		nextTx = 0
	}

	wal := &localWAL{
		base:    base,
		fh:      fh,
		loc:     loc,
		maxSize: maxSize,

		currSize:    currSize,
		startQueue1: startQueue1,
		endQueue1:   endQueue1,
		startQueue2: startQueue2,
		nextTx:      nextTx,
		txToPos:     txToPos,
		keyToTx:     keyToTx,

		wake: make(chan struct{}),
	}
	go wal.drain()
	go func() {
		for {
			time.Sleep(10 * time.Second)
			wal.count()
		}
	}()

	return wal, nil
}

func (lw *localWAL) drain() {
	tick := time.Tick(5 * time.Second)

	for {
		select {
		case <-tick:
		case <-lw.wake:
		}

		if err := lw.drainOnce(); err != nil {
			log.Println(err)
		}
	}
}

func (lw *localWAL) drainOnce() error {
	// Fully drain queue 1.
	lw.mu.Lock()
	start, end := lw.startQueue1, lw.endQueue1
	lw.mu.Unlock()

	for start < end {
		tx, n, err := lw.readQueue(start)
		if err != nil {
			return err
		} else if err := lw.processTx(start, tx); err != nil {
			return err
		}
		lw.startQueue1 += int64(n)
		start, end = lw.startQueue1, lw.endQueue1
		if start == end {
			lw.startQueue1, lw.endQueue1 = 0, 0
		}
		lw.mu.Unlock() // mu was left locked by processTx.
	}

	// Fully drain queue 2.
	lw.mu.Lock()
	pos := lw.startQueue2
	lw.mu.Unlock()

	for {
		tx, n, err := lw.readQueue(pos)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		} else if err := lw.processTx(pos, tx); err != nil {
			return err
		}
		lw.startQueue2 += int64(n)
		pos = lw.startQueue2
		lw.mu.Unlock() // mu was left locked by processTx.
	}

	return nil
}

func (lw *localWAL) readQueue(pos int64) (*transaction, int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	if _, err := lw.fh.Seek(pos, io.SeekStart); err != nil {
		return nil, 0, err
	}
	tx, n, err := readTransaction(lw.fh)
	if err != nil {
		return nil, 0, err
	} else if tx.Processed {
		return nil, 0, fmt.Errorf("read already processed entry")
	}
	return tx, n, nil
}

func (lw *localWAL) processTx(pos int64, tx *transaction) error {
	// Omit any keys that we know have been overwritten in another transaction.
	writes := make(map[string][]byte)

	lw.mu.Lock()
	for key, val := range tx.Data {
		cand, ok := lw.keyToTx[key]
		if !ok || cand != tx.Id {
			continue
		}
		writes[key] = val
	}
	lw.mu.Unlock()

	// Push all these writes downstream.
	ctx := context.Background()
	for key, val := range writes {
		if val == nil {
			if err := lw.base.Delete(ctx, key); err != nil {
				return err
			}
		} else {
			if err := lw.base.Set(ctx, key, val); err != nil {
				return err
			}
		}
	}

	lw.mu.Lock()
	// defer lw.mu.Unlock() -- Intentionally left locked.

	// Mark this transaction as processed.
	if _, err := lw.fh.Seek(pos, io.SeekStart); err != nil {
		lw.mu.Unlock()
		return err
	} else if _, err := lw.fh.Write([]byte{1}); err != nil {
		lw.mu.Unlock()
		return err
	}

	lw.currSize -= len(tx.Data)

	// Delete the keys from `keyToTx` that are removed from the WAL now that
	// this transaction is processed.
	for key, _ := range tx.Data {
		cand, ok := lw.keyToTx[key]
		if !ok || cand != tx.Id {
			continue
		}
		delete(lw.keyToTx, key)
	}

	delete(lw.txToPos, tx.Id)
	return nil
}

func (lw *localWAL) count() int {
	lw.mu.Lock()
	count := lw.currSize
	lw.mu.Unlock()

	LocalWALSize.WithLabelValues(lw.loc).Set(float64(count))
	return count
}

func (lw *localWAL) Start(ctx context.Context) error {
	// Block until the database has drained enough to accept new writes.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		if lw.count() > lw.maxSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case lw.wake <- struct{}{}:
			case <-ticker.C:
			}
			continue
		}
		return nil
	}
}

func (lw *localWAL) Get(ctx context.Context, key string) ([]byte, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	tx, ok := lw.keyToTx[key]
	if !ok {
		return lw.base.Get(ctx, key)
	}
	pos, ok := lw.txToPos[tx]
	if !ok {
		panic("key is stored in unknown transaction")
	}

	if _, err := lw.fh.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}
	t, _, err := readTransaction(lw.fh)
	if err != nil {
		return nil, err
	}
	val, ok := t.Data[key]
	if !ok {
		panic("transaction does not contain expected key")
	}

	return val, nil
}

func (lw *localWAL) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte)
	for _, key := range keys {
		val, err := lw.Get(ctx, key)
		if err == ErrObjectNotFound {
			continue
		} else if err != nil {
			return nil, err
		}
		out[key] = val
	}
	return out, nil
}

func (lw *localWAL) Commit(ctx context.Context, writes map[string][]byte) error {
	if len(writes) == 0 {
		return nil
	}
	lw.mu.Lock()
	defer lw.mu.Unlock()

	id := lw.nextTx
	pos := int64(0)

	buff := &bytes.Buffer{}
	if err := writeTransaction(buff, &transaction{false, id, writes}); err != nil {
		return err
	}

	if gap := int(lw.startQueue2 - lw.endQueue1); gap > buff.Len()+17 {
		// Add this data at the end of queue 1, since there's enough room.
		pos = lw.endQueue1
		endPos := lw.endQueue1 + int64(buff.Len())

		emptyTx := make([]byte, 17)
		emptyTx[0] = 1
		writeInt(gap-buff.Len()-17, emptyTx[9:17])

		if _, err := lw.fh.Seek(pos, io.SeekStart); err != nil {
			return err
		} else if _, err := buff.WriteTo(lw.fh); err != nil { // Write the actual data.
			return err
		} else if _, err := lw.fh.Write(emptyTx); err != nil { // Write an empty transaction to mark the end of queue1.
			return err
		}

		lw.endQueue1 = endPos
	} else {
		// Add this data at the end of the file.
		var err error
		if pos, err = lw.fh.Seek(0, io.SeekEnd); err != nil {
			return err
		} else if _, err := buff.WriteTo(lw.fh); err != nil {
			return err
		}
	}

	lw.currSize += len(writes)
	lw.nextTx += 1
	lw.txToPos[id] = pos
	for key, _ := range writes {
		lw.keyToTx[key] = id
	}
	return nil
}
