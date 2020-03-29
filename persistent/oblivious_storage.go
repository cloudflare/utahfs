package persistent

import (
	"context"
	"database/sql"
	"os"
	"path"

	_ "github.com/mattn/go-sqlite3"
)

type obliviousStore struct {
	base ObliviousStorage

	Stash map[uint64][]byte
	Count uint64

	assignments map[uint64]uint64
}

// newObliviousStore wraps an implementation of the simpler ObliviousStorage
// interface and makes it a bit more user-friendly.
func newObliviousStore(base ObliviousStorage) *obliviousStore {
	return &obliviousStore{base: base}
}

func (os *obliviousStore) Start(ctx context.Context) error {
	stash, count, err := os.base.Start(ctx)
	if err != nil {
		return err
	}

	os.Stash = stash
	os.Count = count
	os.assignments = make(map[uint64]uint64)

	return nil
}

func (os *obliviousStore) Lookup(ctx context.Context, ptrs []uint64) (map[uint64]uint64, error) {
	out := make(map[uint64]uint64)

	// Get as many pointers as possible from the local `assignments` field.
	remaining := make([]uint64, 0)
	for _, ptr := range ptrs {
		if leaf, ok := os.assignments[ptr]; ok {
			out[ptr] = leaf
		} else {
			remaining = append(remaining, ptr)
		}
	}
	if len(remaining) == 0 {
		return out, nil
	}

	// Fetch the remaining from the base implementation.
	part, err := os.base.Lookup(ctx, remaining)
	if err != nil {
		return nil, err
	}
	for ptr, leaf := range part {
		out[ptr] = leaf
	}

	return out, nil
}

type localOblivious struct {
	db *sql.DB
	tx *sql.Tx
}

func NewLocalOblivious(loc string) (ObliviousStorage, error) {
	if err := os.MkdirAll(path.Dir(loc), 0744); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", loc)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS stash (ptr integer not null primary key, val bytea)")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS position (ptr integer not null primary key, leaf integer)")
	if err != nil {
		return nil, err
	}

	return &localOblivious{db: db}, nil
}

func (lo *localOblivious) Start(ctx context.Context) (map[uint64][]byte, uint64, error) {
	tx, err := lo.db.Begin()
	if err != nil {
		return nil, 0, err
	}
	var count int
	err = local.QueryRow("SELECT COUNT(*) FROM position").Scan(&count) // use max instead?
	if err != nil {
		return nil, err
	}
}

func (lo *localOblivious) Lookup(ctx context.Context, ptr []uint64) (map[uint64]uint64, error) {

}

func (lo *localOblivious) Commit(ctx context.Context, stash map[uint64][]byte, assignments map[uint64]uint64) error {

}
