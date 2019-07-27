// Package persistent implements several compatible object-storage backends, and
// additional functionality that can be layered upon them.
package persistent

import (
	"context"
	"errors"
	"sync"
)

const nilPtr = ^uint64(0)

var (
	ErrObjectNotFound = errors.New("object not found")
)

// ObjectStorage defines the minimal interface that's implemented by a remote
// object storage provider.
type ObjectStorage interface {
	// Get returns the data corresponding to the given key, or ErrObjectNotFound
	// if no object with that key exists.
	Get(ctx context.Context, key string) (data []byte, err error)
	// Set updates the object with the given key or creates the object if it
	// does not exist.
	Set(ctx context.Context, key string, data []byte) (err error)
	// Delete removes the object with the given key.
	Delete(ctx context.Context, key string) (err error)
}

// ReliableStorage is an extension of the ObjectStorage interface that provides
// distributed locking (if necessary) and atomic transactions.
type ReliableStorage interface {
	// Start begins a new transaction. The methods below will not work until
	// this is called, and will stop working again after Commit is called.
	//
	// Transactions are isolated and atomic.
	Start(ctx context.Context) error

	Get(ctx context.Context, key string) (data []byte, err error)
	GetMany(ctx context.Context, keys []string) (data map[string][]byte, err error)

	// Commit persists the changes in `writes` to the backend, atomically. If
	// the value of a key is nil, then that key is deleted.
	Commit(ctx context.Context, writes map[string][]byte) error
}

// BlockStorage is a derivative of ObjectStorage that uses uint64 pointers as
// keys instead of strings. It is meant to help make implementing ORAM easier.
type BlockStorage interface {
	Start(ctx context.Context) error

	Get(ctx context.Context, ptr uint64) (data []byte, err error)
	GetMany(ctx context.Context, ptr []uint64) (data map[uint64][]byte, err error)
	Set(ctx context.Context, ptr uint64, data []byte) (err error)

	Commit(ctx context.Context) error
	Rollback(ctx context.Context)
}

func dup(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

// MapMutex implements the ability to lock and unlock specific keys of a map.
type MapMutex struct {
	m *sync.Map
}

func NewMapMutex() MapMutex {
	return MapMutex{m: &sync.Map{}}
}

func (mm MapMutex) Lock(key interface{}) {
	for {
		mu := &sync.Mutex{}
		mu.Lock()

		temp, _ := mm.m.LoadOrStore(key, mu)
		cand := temp.(*sync.Mutex)
		if cand == mu {
			return
		}

		cand.Lock() // Block until the key is unlocked and then try again.
		cand.Unlock()
	}
}

func (mm MapMutex) Unlock(key interface{}) {
	temp, ok := mm.m.Load(key)
	if !ok {
		panic("kmutex: unlock of unlocked mutex")
	}
	mu := temp.(*sync.Mutex)
	mm.m.Delete(key)
	mu.Unlock()
}
