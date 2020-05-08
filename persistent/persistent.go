// Package persistent implements several compatible object-storage backends, and
// additional functionality that can be layered upon them.
package persistent

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const nilPtr = ^uint64(0)

// DataType represents the semantics of the data in a Set operation. It helps
// layers lower in the stack make smarter caching / performance decisions.
type DataType int

const (
	Unknown DataType = iota
	Metadata
	Content
)

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
	Set(ctx context.Context, key string, data []byte, dt DataType) (err error)
	// Delete removes the object with the given key.
	Delete(ctx context.Context, key string) (err error)
}

type WriteData struct {
	Data []byte
	Type DataType
}

// ReliableStorage is an extension of the ObjectStorage interface that provides
// distributed locking (if necessary) and atomic transactions.
type ReliableStorage interface {
	// Start begins a new transaction. The methods below will not work until
	// this is called, and will stop working again after Commit is called.
	//
	// Transactions are isolated and atomic.
	Start(ctx context.Context, prefetch []uint64) (data map[uint64][]byte, err error)

	Get(ctx context.Context, key uint64) (data []byte, err error)
	GetMany(ctx context.Context, keys []uint64) (data map[uint64][]byte, err error)

	// Commit persists the changes in `writes` to the backend, atomically. If
	// the value of a key is nil, then that key is deleted.
	Commit(ctx context.Context, writes map[uint64]WriteData) error
}

// BlockStorage is a derivative of ObjectStorage that uses uint64 pointers as
// keys instead of strings. It is meant to help make implementing ORAM easier.
type BlockStorage interface {
	Start(ctx context.Context, prefetch []uint64) (data map[uint64][]byte, err error)

	Get(ctx context.Context, ptr uint64) (data []byte, err error)
	GetMany(ctx context.Context, ptrs []uint64) (data map[uint64][]byte, err error)
	Set(ctx context.Context, ptr uint64, data []byte, dt DataType) (err error)

	Commit(ctx context.Context) error
	Rollback(ctx context.Context)
}

// ObliviousStorage defines the interface an ORAM implementation would use to
// access and store sensitive data.
type ObliviousStorage interface {
	// Start begins a new transaction. It returns the number of blocks and the
	// current stash.
	Start(ctx context.Context, version uint64) (stash map[uint64][]byte, size uint64, err error)

	// Lookup returns a map from each requested pointer, to the leaf that the
	// pointer is assigned to.
	Lookup(ctx context.Context, ptr []uint64) (map[uint64]uint64, error)

	// Commit ends the transaction, replacing the stash with the given map and
	// making the requested pointer-to-leaf assignments.
	Commit(ctx context.Context, version uint64, stash map[uint64][]byte, assignments map[uint64]uint64) error

	// Rollback aborts the transaction without attempting to make any changes.
	Rollback(ctx context.Context)
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

func dup(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func hex(key uint64) string {
	return fmt.Sprintf("%x", key)
}

// The functions below are all for left-balanced binary tree math. They're taken
// from the MLS specification.

// log2 returns the exponent of the largest power of 2 less than x.
func log2(x uint64) uint64 {
	if x == 0 {
		return 0
	}

	var k uint64
	for (x >> k) > 0 {
		k += 1
	}
	return k - 1
}

// level returns the level of a node in the tree.
func level(x uint64) uint64 {
	if x&1 == 0 {
		return 0
	}

	var k uint64
	for (x>>k)&1 == 1 {
		k += 1
	}
	return k
}

// treeWidth returns the number of nodes needed to represent a tree with n
// leaves.
func treeWidth(n uint64) uint64 { return 2*(n-1) + 1 }

// rootNode returns the index of the root node of a tree with n leaves.
func rootNode(n uint64) uint64 {
	w := treeWidth(n)
	return (1 << log2(w)) - 1
}

// parentStep returns the immediate parent of a node.
func parentStep(x uint64) uint64 {
	k := level(x)
	b := (x >> (k + 1)) & 1
	return (x | (1 << k)) ^ (b << (k + 1))
}
