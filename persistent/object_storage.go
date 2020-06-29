package persistent

import (
	"context"
	"errors"
)

type memory map[string][]byte

// NewMemory returns an object storage backend that simply stores data
// in-memory.
func NewMemory() ObjectStorage { return make(memory) }

func (m memory) Get(ctx context.Context, key string) ([]byte, error) {
	data, ok := m[key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return dup(data), nil
}

func (m memory) Set(ctx context.Context, key string, data []byte, dt DataType) error {
	m[key] = dup(data)
	return nil
}

func (m memory) Delete(ctx context.Context, key string) error {
	delete(m, key)
	return nil
}

type retry struct {
	base     ObjectStorage
	attempts int
}

// NewRetry wraps a base object storage backend, and will retry if requests
// fail.
func NewRetry(base ObjectStorage, attempts int) (ObjectStorage, error) {
	if attempts <= 0 {
		return nil, errors.New("storage: attempts must be greater than zero")
	}
	return &retry{base, attempts}, nil
}

func (r *retry) Get(ctx context.Context, key string) (data []byte, err error) {
	for i := 0; i < r.attempts; i++ {
		data, err = r.base.Get(ctx, key)
		if err == nil || err == ErrObjectNotFound {
			return
		}
	}

	return
}

func (r *retry) Set(ctx context.Context, key string, data []byte, dt DataType) (err error) {
	for i := 0; i < r.attempts; i++ {
		err = r.base.Set(ctx, key, data, dt)
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

type prefix struct {
	base   ObjectStorage
	prefix string
}

// NewPrefix wraps a base object storage backend, and ensures a user-provided
// prefix is added to all keys.
func NewPrefix(base ObjectStorage, p string) ObjectStorage {
	return &prefix{base, p}
}

func (p *prefix) Get(ctx context.Context, key string) ([]byte, error) {
	return p.base.Get(ctx, p.prefix+key)
}

func (p *prefix) Set(ctx context.Context, key string, data []byte, dt DataType) error {
	return p.base.Set(ctx, p.prefix+key, data, dt)
}

func (p *prefix) Delete(ctx context.Context, key string) error {
	return p.base.Delete(ctx, p.prefix+key)
}
