// Package storage implements handlers for compatible object-storage backends.
package storage

import (
	"context"
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

func (r *retry) Get(ctx context.Context, key string) (data []byte, err error) {
	for i := 0; i < r.attempts; i++ {
		data, err = r.base.Get(ctx, key)
		if err == nil || err == utahfs.ErrObjectNotFound {
			return
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	return
}

func (r *retry) Set(ctx context.Context, key string, data []byte) (err error) {
	for i := 0; i < r.attempts; i++ {
		err = r.base.Set(ctx, key, data)
		if err == nil {
			return
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return
}

func (r *retry) Delete(ctx context.Context, key string) (err error) {
	for i := 0; i < r.attempts; i++ {
		err = r.base.Delete(ctx, key)
		if err == nil {
			return
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return
}
