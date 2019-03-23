package utahfs

import (
	"errors"
)

var (
	ErrObjectNotFound = errors.New("object not found")
)

// ObjectStorage defines the minimal interface that's implemented by a remote
// object storage provider.
type ObjectStorage interface {
	// Get returns the data corresponding to the given key, or ErrObjectNotFound
	// if no object with that key exists.
	Get(key string) (data []byte, err error)
	// Set updates the object with the given key or creates the object if it
	// does not exist.
	Set(key string, data []byte) (err error)
	// Delete removes the object with the given key.
	Delete(key string) (err error)
}
