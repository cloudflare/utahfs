// Package storage implements handlers for compatible object-storage backends.
package storage

import (
	"errors"

	"github.com/Bren2010/utahfs"
)

type retry struct {
	base     utahfs.ObjectStorage
	attempts int
}

// NewRetry wraps a base object storage backend, and will retry if requests
// fail.
func NewRetry(base utahfs.ObjectStorage, attempts int) (utahfs.ObjectStorage, error) {
	if attempts <= 0 {
		return nil, errors.New("attempts must be greater than zero")
	}
	return &retry{base, attempts}, nil
}

func (r *retry) Get(key string) (data []byte, err error) {
	for i := 0; i < r.attempts; i++ {
		data, err = r.base.Get(key)
		if err == nil || err == utahfs.ErrObjectNotFound {
			return
		}
	}

	return
}

func (r *retry) Set(key string, data []byte) (err error) {
	for i := 0; i < r.attempts; i++ {
		err = r.base.Set(key, data)
		if err == nil {
			return
		}
	}

	return
}

func (r *retry) Delete(key string) (err error) {
	for i := 0; i < r.attempts; i++ {
		err = r.base.Delete(key)
		if err == nil {
			return
		}
	}

	return
}
