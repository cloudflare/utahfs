package persistent

import (
	"bytes"
	"context"

	"github.com/cloudflare/utahfs/cache"
)

type simpleReliable struct {
	base ObjectStorage
}

// NewSimpleReliable returns a ReliableStorage implementation, intended for
// testing. It simply panics if the atomicity of a transaction is broken.
func NewSimpleReliable(base ObjectStorage) ReliableStorage {
	return &simpleReliable{base}
}

func (sr *simpleReliable) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	return sr.GetMany(ctx, prefetch)
}

func (sr *simpleReliable) Get(ctx context.Context, key uint64) ([]byte, error) {
	return sr.base.Get(ctx, hex(key))
}

func (sr *simpleReliable) GetMany(ctx context.Context, keys []uint64) (map[uint64][]byte, error) {
	out := make(map[uint64][]byte)
	for _, key := range keys {
		val, err := sr.Get(ctx, key)
		if err == ErrObjectNotFound {
			continue
		} else if err != nil {
			return nil, err
		}
		out[key] = val
	}
	return out, nil
}

func (sr *simpleReliable) Commit(ctx context.Context, writes map[uint64]WriteData) error {
	for key, wr := range writes {
		if err := sr.base.Set(ctx, hex(key), wr.Data, wr.Type); err != nil {
			panic(err)
		}
	}
	return nil
}

type cacheStorage struct {
	base  ReliableStorage
	cache *cache.Cache
}

// NewCache wraps a base object storage backend with an LRU cache of the
// requested size.
func NewCache(base ReliableStorage, size int) ReliableStorage {
	return &cacheStorage{
		base:  base,
		cache: cache.New(cache.NoExpiration, 0, size),
	}
}

func (c *cacheStorage) filterCached(keys []uint64) (out map[uint64][]byte, remaining []uint64) {
	out = make(map[uint64][]byte)
	remaining = make([]uint64, 0)

	for _, key := range keys {
		val, ok := c.cache.Get(key)
		if ok {
			out[key] = dup(val.([]byte))
			continue
		}
		remaining = append(remaining, key)
	}

	return
}

func (c *cacheStorage) cacheAndOutput(data, out map[uint64][]byte) {
	for key, val := range data {
		out[key] = val
		c.cache.Set(key, dup(val), cache.NoExpiration)
	}
}

func (c *cacheStorage) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	out, remaining := c.filterCached(prefetch)

	data, err := c.base.Start(ctx, remaining)
	if err != nil {
		return nil, err
	}
	c.cacheAndOutput(data, out)

	return out, nil
}

func (c *cacheStorage) Get(ctx context.Context, key uint64) ([]byte, error) {
	data, err := c.GetMany(ctx, []uint64{key})
	if err != nil {
		return nil, err
	} else if data[key] == nil {
		return nil, ErrObjectNotFound
	}
	return data[key], nil
}

func (c *cacheStorage) GetMany(ctx context.Context, keys []uint64) (map[uint64][]byte, error) {
	out, remaining := c.filterCached(keys)

	if len(remaining) > 0 {
		data, err := c.base.GetMany(ctx, remaining)
		if err != nil {
			return nil, err
		}
		c.cacheAndOutput(data, out)
	}

	return out, nil
}

func (c *cacheStorage) skip(key uint64, data []byte) bool {
	cand, ok := c.cache.Get(key)
	return ok && bytes.Equal(cand.([]byte), data)
}

func (c *cacheStorage) Commit(ctx context.Context, writes map[uint64]WriteData) error {
	dedupedWrites := make(map[uint64]WriteData)
	for key, wr := range writes {
		if c.skip(key, wr.Data) {
			continue
		}
		dedupedWrites[key] = wr
	}

	if err := c.base.Commit(ctx, dedupedWrites); err != nil {
		return err
	}

	for key, wr := range dedupedWrites {
		if wr.Data == nil {
			c.cache.Delete(key)
		} else {
			c.cache.Set(key, dup(wr.Data), cache.NoExpiration)
		}
	}
	return nil
}

type blockReliable struct {
	BlockStorage
}

// NewBlockReliable returns a ReliableStorage implementation based on a
// BlockStorage implementation.
func NewBlockReliable(base BlockStorage) ReliableStorage {
	return &blockReliable{base}
}

func (br *blockReliable) Commit(ctx context.Context, writes map[uint64]WriteData) error {
	for ptr, wd := range writes {
		if err := br.Set(ctx, ptr, wd.Data, wd.Type); err != nil {
			return err
		}
	}
	return br.BlockStorage.Commit(ctx)
}
