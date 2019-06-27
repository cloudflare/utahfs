package persistent

import (
	"context"
	"fmt"
)

type changes struct {
	Writes map[string][]byte
}

func newChanges() *changes {
	return &changes{Writes: make(map[string][]byte)}
}

// BufferedStorage is an extension of the ReliableStorage interface that will
// buffer many changes and then commit them all at once.
type BufferedStorage struct {
	base    ReliableStorage
	pending *changes
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
	bs.pending = newChanges()

	return nil
}

func (bs *BufferedStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if bs.pending == nil {
		return nil, fmt.Errorf("app: transaction not active")
	}

	if data, ok := bs.pending.Writes[key]; ok {
		if data != nil {
			return dup(data), nil
		}
		return nil, ErrObjectNotFound
	}
	return bs.base.Get(ctx, key)
}

func (bs *BufferedStorage) Set(ctx context.Context, key string, data []byte) error {
	if bs.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}
	bs.pending.Writes[key] = dup(data)
	return nil
}

func (bs *BufferedStorage) Delete(ctx context.Context, key string) error {
	if bs.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}
	bs.pending.Writes[key] = nil
	return nil
}

// Commit persists any changes made to the backend.
func (bs *BufferedStorage) Commit(ctx context.Context) error {
	if bs.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}

	if err := bs.base.Commit(ctx, bs.pending.Writes); err != nil {
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
