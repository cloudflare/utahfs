package persistent

import (
	"container/heap"
	"context"
	"log"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
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
	mu sync.Mutex

	base ObjectStorage
	size int

	keys *keysHeap
	db   *leveldb.DB
}

// NewDiskCache wraps a base object storage backend with a large on-disk LRU
// cache stored at `path`.
func NewDiskCache(base ObjectStorage, path string, size int) (ObjectStorage, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	// List all keys in the cache and build a heap.
	kh := newKeysHeap(size)

	iter := db.NewIterator(&util.Range{nil, nil}, nil)
	for iter.Next() {
		kh.Push(string(iter.Key()))
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}

	heap.Init(kh)

	return &diskCache{
		base: base,
		size: size,

		keys: kh,
		db:   db,
	}, nil
}

func (dc *diskCache) addToCache(key string, data []byte) {
	// Add this key to the cache.
	if err := dc.db.Put([]byte(key), data, nil); err != nil {
		log.Println(err)
		return
	}
	dc.keys.bump(key)

	// Evict from the cache until we're back at/below the target size.
	for dc.keys.Len() > dc.size {
		key := heap.Pop(dc.keys).(string)
		if err := dc.db.Delete([]byte(key), nil); err != nil {
			log.Println(err)
			return
		}
	}
}

func (dc *diskCache) removeFromCache(key string) {
	dc.keys.remove(key)
	if err := dc.db.Delete([]byte(key), nil); err != nil {
		log.Println(err)
	}
}

func (dc *diskCache) Get(ctx context.Context, key string) ([]byte, error) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	data, err := dc.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		data, err = dc.base.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		dc.addToCache(key, data)
		return data, nil
	} else if err != nil {
		return nil, err
	}
	dc.keys.bump(key)
	return data, nil
}

func (dc *diskCache) Set(ctx context.Context, key string, data []byte) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if err := dc.base.Set(ctx, key, data); err != nil {
		dc.removeFromCache(key)
		return err
	}
	dc.addToCache(key, data)
	return nil
}

func (dc *diskCache) Delete(ctx context.Context, key string) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.removeFromCache(key)
	return dc.base.Delete(ctx, key)
}
