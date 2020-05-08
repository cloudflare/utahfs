package persistent

import (
	"context"
)

type blockMemory map[uint64][]byte

// NewBlockMemory returns an implementation of BlockStorage that simply stores
// data in-memory.
func NewBlockMemory() BlockStorage { return make(blockMemory) }

func (bm blockMemory) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	return bm.GetMany(ctx, prefetch)
}

func (bm blockMemory) Get(ctx context.Context, ptr uint64) ([]byte, error) {
	d, ok := bm[ptr]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return dup(d), nil
}

func (bm blockMemory) GetMany(ctx context.Context, ptrs []uint64) (map[uint64][]byte, error) {
	out := make(map[uint64][]byte)
	for _, ptr := range ptrs {
		val, ok := bm[ptr]
		if !ok {
			continue
		}
		out[ptr] = dup(val)
	}
	return out, nil
}

func (bm blockMemory) Set(ctx context.Context, ptr uint64, data []byte, _ DataType) error {
	bm[ptr] = dup(data)
	return nil
}

func (bm blockMemory) Commit(ctx context.Context) error { return nil }
func (bm blockMemory) Rollback(ctx context.Context)     {}
