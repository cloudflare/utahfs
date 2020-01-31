package persistent

import (
	"bytes"
	"context"

	"github.com/hashicorp/golang-lru"
)

type simpleReliable struct {
	base ObjectStorage
}

// NewSimpleReliable returns a ReliableStorage implementation, intended for
// testing. It simply panics if the atomicity of a transaction is broken.
func NewSimpleReliable(base ObjectStorage) ReliableStorage {
	return &simpleReliable{base}
}

func (sr *simpleReliable) Start(ctx context.Context, prefetch []string) (map[string][]byte, error) {
	return sr.GetMany(ctx, prefetch)
}

func (sr *simpleReliable) Get(ctx context.Context, key string) ([]byte, error) {
	return sr.base.Get(ctx, key)
}

func (sr *simpleReliable) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte)
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

func (sr *simpleReliable) Commit(ctx context.Context, writes map[string]WriteData) error {
	for key, wr := range writes {
		if err := sr.base.Set(ctx, key, wr.Data, wr.Type); err != nil {
			panic(err)
		}
	}
	return nil
}

type cache struct {
	base  ReliableStorage
	cache *lru.TwoQueueCache
}

// NewCache wraps a base object storage backend with an LRU cache of the
// requested size.
func NewCache(base ReliableStorage, size int) (ReliableStorage, error) {
	c, err := lru.New2Q(size)
	if err != nil {
		return nil, err
	}
	return &cache{base, c}, nil
}

func (c *cache) filterCached(keys []string) (out map[string][]byte, remaining []string) {
	out = make(map[string][]byte)
	remaining = make([]string, 0)

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

func (c *cache) cacheAndOutput(data, out map[string][]byte) {
	for key, val := range data {
		out[key] = val
		c.cache.Add(key, dup(val))
	}
}

func (c *cache) Start(ctx context.Context, prefetch []string) (map[string][]byte, error) {
	out, remaining := c.filterCached(prefetch)

	data, err := c.base.Start(ctx, remaining)
	if err != nil {
		return nil, err
	}
	c.cacheAndOutput(data, out)

	return out, nil
}

func (c *cache) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := c.GetMany(ctx, []string{key})
	if err != nil {
		return nil, err
	} else if data[key] == nil {
		return nil, ErrObjectNotFound
	}
	return data[key], nil
}

func (c *cache) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
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

func (c *cache) skip(key string, data []byte) bool {
	cand, ok := c.cache.Get(key)
	return ok && bytes.Equal(cand.([]byte), data)
}

func (c *cache) Commit(ctx context.Context, writes map[string]WriteData) error {
	dedupedWrites := make(map[string]WriteData)
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
			c.cache.Remove(key)
		} else {
			c.cache.Add(key, dup(wr.Data))
		}
	}
	return nil
}
