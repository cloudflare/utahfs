package persistent

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/prometheus/client_golang/prometheus"
)

var LocalWALSize = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "local_wal_size",
		Help: "The number of entries in the local WAL.",
	},
	[]string{"path"},
)

type localWAL struct {
	mu sync.Mutex

	base  ObjectStorage
	local *leveldb.DB

	path      string
	maxSize   int
	currSize  int
	lastCount time.Time
	wake      chan struct{}
}

// NewLocalWAL returns a ReliableStorage implementation that achieves reliable
// writes over a base object storage provider by buffering writes in a
// Write-Ahead Log (WAL) stored at `path`.
//
// The WAL may have at least `maxSize` buffered entries before new writes start
// blocking on old writes being flushed.
func NewLocalWAL(base ObjectStorage, path string, maxSize int) (ReliableStorage, error) {
	local, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	wal := &localWAL{
		base:  base,
		local: local,

		path:      path,
		maxSize:   maxSize,
		currSize:  0,
		lastCount: time.Time{},
		wake:      make(chan struct{}),
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
	iter := lw.local.NewIterator(&util.Range{nil, nil}, nil)
	for i := 0; iter.Next(); i++ {
		key, val := iter.Key(), iter.Value()

		if val != nil {
			if err := lw.base.Set(context.Background(), string(key), val); err != nil {
				return err
			}
		} else {
			if err := lw.base.Delete(context.Background(), string(key)); err != nil {
				return err
			}
		}
		if err := lw.drop(key, val); err != nil {
			return err
		}
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return err
	}

	return nil
}

func (lw *localWAL) drop(key, val []byte) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	cand, err := lw.local.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	} else if !bytes.Equal(val, cand) {
		return nil
	}
	return lw.local.Delete(key, nil)
}

func (lw *localWAL) count() (int, error) {
	lw.mu.Lock()
	if time.Since(lw.lastCount) < 10*time.Second {
		curr := lw.currSize
		lw.mu.Unlock()
		return curr, nil
	}
	lw.mu.Unlock()

	count := 0
	iter := lw.local.NewIterator(&util.Range{nil, nil}, nil)
	for iter.Next() {
		count++
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return 0, err
	}

	lw.mu.Lock()
	lw.lastCount = time.Now()
	lw.currSize = count
	lw.mu.Unlock()

	LocalWALSize.WithLabelValues(lw.path).Set(float64(count))
	return count, nil
}

func (lw *localWAL) Start(ctx context.Context) error {
	// Block until the database has drained enough to accept new writes.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		count, err := lw.count()
		if err != nil {
			return err
		}

		if count > lw.maxSize {
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
	data, err := lw.local.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return lw.base.Get(ctx, key)
	} else if data == nil {
		return nil, ErrObjectNotFound
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func (lw *localWAL) Commit(ctx context.Context, writes map[string][]byte) error {
	if len(writes) == 0 {
		return nil
	}

	batch := new(leveldb.Batch)
	for key, val := range writes {
		batch.Put([]byte(key), dup(val))
	}
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if err := lw.local.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		return err
	}
	return nil
}
