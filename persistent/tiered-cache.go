package persistent

import (
	"context"
)

type tieredCache struct {
	special DataType

	high, base ObjectStorage
}

// NewTieredCache returns a cache-like object storage implementation, where
// objects matching the `special` data type are stored in both `high` and
// `base`, while all other objects are stored only in `base`.
func NewTieredCache(special DataType, high, base ObjectStorage) ObjectStorage {
	return &tieredCache{
		special: special,

		high: high,
		base: base,
	}
}

func (tc *tieredCache) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := tc.high.Get(ctx, key)
	if err == ErrObjectNotFound {
		return tc.base.Get(ctx, key)
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func (tc *tieredCache) Set(ctx context.Context, key string, data []byte, dt DataType) error {
	if dt == tc.special {
		if err := tc.high.Set(ctx, key, data, dt); err != nil {
			return err
		}
	}
	return tc.base.Set(ctx, key, data, dt)
}

func (tc *tieredCache) Delete(ctx context.Context, key string) error {
	if err := tc.high.Delete(ctx, key); err != nil {
		return err
	}
	return tc.base.Delete(ctx, key)
}
