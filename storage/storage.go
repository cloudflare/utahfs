// Package storage implements handlers for compatible object-storage backends.
package storage

import (
	"bytes"
	"context"
	"errors"

	"github.com/Bren2010/utahfs"

	"github.com/hashicorp/golang-lru"
)

func dup(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

type memory map[string][]byte

// NewMemory returns an object storage backend that simply stores data
// in-memory.
func NewMemory() utahfs.ObjectStorage {
	return make(memory)
}

func (m memory) Get(ctx context.Context, key string) ([]byte, error) {
	data, ok := m[key]
	if !ok {
		return nil, utahfs.ErrObjectNotFound
	}
	return dup(data), nil
}

func (m memory) Set(ctx context.Context, key string, data []byte) error {
	m[key] = dup(data)
	return nil
}

func (m memory) Delete(ctx context.Context, key string) error {
	delete(m, key)
	return nil
}

type retry struct {
	base     utahfs.ObjectStorage
	attempts int
}

// NewRetry wraps a base object storage backend, and will retry if requests
// fail.
func NewRetry(base utahfs.ObjectStorage, attempts int) (utahfs.ObjectStorage, error) {
	if attempts <= 0 {
		return nil, errors.New("storage: attempts must be greater than zero")
	}
	return &retry{base, attempts}, nil
}

func (r *retry) Get(ctx context.Context, key string) (data []byte, err error) {
	for i := 0; i < r.attempts; i++ {
		data, err = r.base.Get(ctx, key)
		if err == nil || err == utahfs.ErrObjectNotFound {
			return
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
	}

	return
}

func (r *retry) Delete(ctx context.Context, key string) (err error) {
	for i := 0; i < r.attempts; i++ {
		err = r.base.Delete(ctx, key)
		if err == nil {
			return
		}
	}

	return
}

type cache struct {
	base  utahfs.ObjectStorage
	cache *lru.Cache
}

// NewCache wraps a base object storage backend with an LRU cache of the
// requested size.
func NewCache(base utahfs.ObjectStorage, size int) (utahfs.ObjectStorage, error) {
	c, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &cache{base, c}, nil
}

func (c *cache) Get(ctx context.Context, key string) ([]byte, error) {
	val, ok := c.cache.Get(key)
	if ok {
		return dup(val.([]byte)), nil
	}
	data, err := c.base.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	c.cache.Add(key, dup(data))
	return data, nil
}

func (c *cache) skip(key string, data []byte) bool {
	cand, ok := c.cache.Get(key)
	if !ok {
		return false
	} else if !bytes.Equal(cand.([]byte), data) {
		return false
	}
	return true
}

func (c *cache) Set(ctx context.Context, key string, data []byte) error {
	if c.skip(key, data) {
		return nil
	}
	c.cache.Remove(key)
	if err := c.base.Set(ctx, key, data); err != nil {
		return err
	}
	c.cache.Add(key, dup(data))
	return nil
}

func (c *cache) Delete(ctx context.Context, key string) error {
	c.cache.Remove(key)
	return c.base.Delete(ctx, key)
}
