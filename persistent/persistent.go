// Package persistent implements several compatible object-storage backends, and
// additional functionality that can be layered upon them.
package persistent

import (
	"context"
	"errors"
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

	// Commit persists the changes in `writes` to the backend, atomically. If
	// the value of a key is nil, then that key is deleted.
	Commit(ctx context.Context, writes map[string][]byte) error
}

// BlockStorage is a derivative of ObjectStorage that uses uint64 pointers as
// keys instead of strings. It is meant to help make implementing ORAM easier.
type BlockStorage interface {
	Start(ctx context.Context) error

	Get(ctx context.Context, ptr uint64) (data []byte, err error)
	Set(ctx context.Context, ptr uint64, data []byte) (err error)

	Commit(ctx context.Context) error
	Rollback(ctx context.Context)
}
