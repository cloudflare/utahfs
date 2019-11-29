package persistent

import (
	"container/heap"
	"context"
	"database/sql"
	"log"
	"os"
	"path"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/prometheus/client_golang/prometheus"
)

var DiskCacheSize = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "disk_cache_size",
		Help: "The number of entries in the on-disk cache.",
	},
	[]string{"path"},
)

type keysHeap struct {
	keys     []string
	lastUsed []int64

	keyToPos map[string]int
}

func newKeysHeap(size int) *keysHeap {
	return &keysHeap{
		keys:     make([]string, 0, size+1),
		lastUsed: make([]int64, 0, size+1),

		keyToPos: make(map[string]int, size+1),
	}
}

func (kh *keysHeap) Len() int { return len(kh.keys) }

func (kh *keysHeap) Less(i, j int) bool {
	return kh.lastUsed[i] < kh.lastUsed[j]
}

func (kh *keysHeap) Swap(i, j int) {
	key1, key2 := kh.keys[i], kh.keys[j]

	kh.keys[i], kh.keys[j] = kh.keys[j], kh.keys[i]
	kh.lastUsed[i], kh.lastUsed[j] = kh.lastUsed[j], kh.lastUsed[i]
	kh.keyToPos[key1], kh.keyToPos[key2] = j, i
}

func (kh *keysHeap) Push(x interface{}) {
	key := x.(string)

	kh.keys = append(kh.keys, key)
	kh.lastUsed = append(kh.lastUsed, time.Now().UnixNano())
	kh.keyToPos[key] = len(kh.keys) - 1
}

func (kh *keysHeap) Pop() interface{} {
	key := kh.keys[len(kh.keys)-1]

	kh.keys = kh.keys[:len(kh.keys)-1]
	kh.lastUsed = kh.lastUsed[:len(kh.lastUsed)-1]
	delete(kh.keyToPos, key)

	return key
}

func (kh *keysHeap) bump(key string) {
	pos, ok := kh.keyToPos[key]
	if !ok {
		heap.Push(kh, key)
		return
	}
	kh.lastUsed[pos] = time.Now().UnixNano()
	heap.Fix(kh, pos)
}

func (kh *keysHeap) remove(key string) {
	pos, ok := kh.keyToPos[key]
	if !ok {
		return
	}
	heap.Remove(kh, pos)
}

type diskCache struct {
	mu    sync.Mutex
	mapMu MapMutex

	base ObjectStorage
	size int
	loc  string

	keys *keysHeap
	db   *sql.DB
}

// NewDiskCache wraps a base object storage backend with a large on-disk LRU
// cache stored at `loc`.
func NewDiskCache(base ObjectStorage, loc string, size int) (ObjectStorage, error) {
	if err := os.MkdirAll(path.Dir(loc), 0744); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", loc)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS cache (key text not null primary key, val bytea);`)
	if err != nil {
		return nil, err
	}

	// List all keys in the cache and build a heap.
	kh := newKeysHeap(size)

	rows, err := db.Query("SELECT key FROM cache;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		kh.Push(key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	heap.Init(kh)

	DiskCacheSize.WithLabelValues(loc).Set(float64(kh.Len()))
	return &diskCache{
		mapMu: NewMapMutex(),

		base: base,
		size: size,
		loc:  loc,

		keys: kh,
		db:   db,
	}, nil
}

func (dc *diskCache) addToCache(key string, data []byte) {
	// Add this key to the cache.
	_, err := dc.db.Exec("INSERT OR REPLACE INTO cache (key, val) VALUES (?, ?)", key, data)
	if err != nil {
		log.Println(err)
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.keys.bump(key)

	// Evict from the cache until we're back at/below the target size.
	for dc.keys.Len() > dc.size {
		key := heap.Pop(dc.keys).(string)
		if _, err := dc.db.Exec("DELETE FROM cache WHERE key = ?", key); err != nil {
			log.Println(err)
			return
		}
	}
	DiskCacheSize.WithLabelValues(dc.loc).Set(float64(dc.keys.Len()))
}

func (dc *diskCache) removeFromCache(key string) {
	dc.mu.Lock()
	dc.keys.remove(key)
	dc.mu.Unlock()
	if _, err := dc.db.Exec("DELETE FROM cache WHERE key = ?", key); err != nil {
		log.Println(err)
	}
	DiskCacheSize.WithLabelValues(dc.loc).Dec()
}

func (dc *diskCache) Get(ctx context.Context, key string) ([]byte, error) {
	dc.mapMu.Lock(key)
	defer dc.mapMu.Unlock(key)

	var data []byte
	err := dc.db.QueryRow("SELECT val FROM cache WHERE key = ?", key).Scan(&data)
	if err == sql.ErrNoRows {
		data, err = dc.base.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		dc.addToCache(key, data)
		return data, nil
	} else if err != nil {
		return nil, err
	}
	dc.mu.Lock()
	dc.keys.bump(key)
	dc.mu.Unlock()
	return data, nil
}

func (dc *diskCache) Set(ctx context.Context, key string, data []byte, dt DataType) error {
	dc.mapMu.Lock(key)
	defer dc.mapMu.Unlock(key)

	if err := dc.base.Set(ctx, key, data, dt); err != nil {
		dc.removeFromCache(key)
		return err
	}
	dc.addToCache(key, data)
	return nil
}

func (dc *diskCache) Delete(ctx context.Context, key string) error {
	dc.mapMu.Lock(key)
	defer dc.mapMu.Unlock(key)

	err := dc.base.Delete(ctx, key)
	dc.removeFromCache(key)
	return err
}
