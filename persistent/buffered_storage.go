package persistent

import (
	"context"
	"fmt"
)

// BufferedStorage is an extension of the ReliableStorage interface that will
// buffer many changes and then commit them all at once.
type BufferedStorage struct {
	base    ReliableStorage
	pending map[string]WriteData
}

func NewBufferedStorage(base ReliableStorage) *BufferedStorage {
	return &BufferedStorage{base: base}
}

func (bs *BufferedStorage) Start(ctx context.Context) error {
	if bs.pending != nil {
		return fmt.Errorf("app: transaction already started")
	}

	if err := bs.base.Start(ctx); err != nil {
		return err
	}
	bs.pending = make(map[string]WriteData)

	return nil
}

func (bs *BufferedStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if bs.pending == nil {
		return nil, fmt.Errorf("app: transaction not active")
	}

	if wr, ok := bs.pending[key]; ok {
		if wr.Data != nil {
			return dup(wr.Data), nil
		}
		return nil, ErrObjectNotFound
	}
	return bs.base.Get(ctx, key)
}

func (bs *BufferedStorage) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	if bs.pending == nil {
		return nil, fmt.Errorf("app: transaction not active")
	}

	out := make(map[string][]byte)
	remaining := make([]string, 0)
	for _, key := range keys {
		if wr, ok := bs.pending[key]; ok {
			if wr.Data != nil {
				out[key] = dup(wr.Data)
			}
			continue
		}
		remaining = append(remaining, key)
	}

	if len(remaining) > 0 {
		data, err := bs.base.GetMany(ctx, remaining)
		if err != nil {
			return nil, err
		}
		for key, val := range data {
			out[key] = val
		}
	}

	return out, nil
}

func (bs *BufferedStorage) Set(ctx context.Context, key string, data []byte, dt DataType) error {
	if bs.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}
	bs.pending[key] = WriteData{Data: dup(data), Type: dt}
	return nil
}

func (bs *BufferedStorage) Delete(ctx context.Context, key string) error {
	if bs.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}
	bs.pending[key] = WriteData{Data: nil}
	return nil
}

// Commit persists any changes made to the backend.
func (bs *BufferedStorage) Commit(ctx context.Context) error {
	if bs.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}

	if err := bs.base.Commit(ctx, bs.pending); err != nil {
		return err
	}
	bs.pending = nil

	return nil
}

// Rollback discards all changes made in this transaction.
func (bs *BufferedStorage) Rollback(ctx context.Context) {
	bs.base.Commit(ctx, nil)
	bs.pending = nil
}
