package storage

import (
	"context"
	"fmt"
)

type simpleBlockStorage struct {
	base *BufferedStorage
}

// NewSimpleBlockStorage turns a BufferedStorage implementation into a
// BlockStorage implementation. It simply converts the block pointer into a hex
// string and uses that as the key.
func NewSimpleBlockStorage(base *BufferedStorage) BlockStorage {
	return simpleBlockStorage{base}
}

func (sbs simpleBlockStorage) Start(ctx context.Context) error {
	return sbs.base.Start(ctx)
}

func (sbs simpleBlockStorage) Get(ctx context.Context, ptr uint32) ([]byte, error) {
	return sbs.base.Get(ctx, fmt.Sprintf("%x", ptr))
}

func (sbs simpleBlockStorage) Set(ctx context.Context, ptr uint32, data []byte) error {
	return sbs.base.Set(ctx, fmt.Sprintf("%x", ptr), data)
}

func (sbs simpleBlockStorage) Commit(ctx context.Context) error {
	return sbs.base.Commit(ctx)
}

func (sbs simpleBlockStorage) Rollback(ctx context.Context) {
	sbs.base.Rollback(ctx)
}
