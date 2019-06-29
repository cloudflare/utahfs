package persistent

import (
	"bytes"
	"context"

	"github.com/hashicorp/golang-lru"
)

type simpleReliableStorage struct {
	base ObjectStorage
}

// NewSimpleReliableStorage returns a ReliableStorage implementation, intended
// for testing. It simply panics if the atomicity of a transaction is broken.
func NewSimpleReliableStorage(base ObjectStorage) ReliableStorage {
	return &simpleReliableStorage{base}
}

func (srs *simpleReliableStorage) Start(ctx context.Context) error { return nil }

func (srs *simpleReliableStorage) Get(ctx context.Context, key string) ([]byte, error) {
	return srs.base.Get(ctx, key)
}

func (srs *simpleReliableStorage) Commit(ctx context.Context, writes map[string][]byte) error {
	for key, val := range writes {
		if err := srs.base.Set(ctx, key, val); err != nil {
			panic(err)
		}
	}
	return nil
}

type cache struct {
	base  ReliableStorage
	cache *lru.Cache
}

// NewCache wraps a base object storage backend with an LRU cache of the
// requested size.
func NewCache(base ReliableStorage, size int) (ReliableStorage, error) {
	c, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &cache{base, c}, nil
}

func (c *cache) Start(ctx context.Context) error { return c.base.Start(ctx) }

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
	return ok && bytes.Equal(cand.([]byte), data)
}

func (c *cache) Commit(ctx context.Context, writes map[string][]byte) error {
	dedupedWrites := make(map[string][]byte)
	for key, data := range writes {
		if c.skip(key, data) {
			continue
		}
		dedupedWrites[key] = data
	}

	if err := c.base.Commit(ctx, dedupedWrites); err != nil {
		return err
	}

	for key, data := range dedupedWrites {
		if data == nil {
			c.cache.Remove(key)
		} else {
			c.cache.Add(key, dup(data))
		}
	}
	return nil
}
