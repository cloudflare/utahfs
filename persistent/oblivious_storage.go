package persistent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

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

func (os *obliviousStore) Start(ctx context.Context, version uint64) error {
	stash, count, err := os.base.Start(ctx, version)
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

func (os *obliviousStore) Assign(assignments map[uint64]uint64) {
	for ptr, leaf := range assignments {
		os.assignments[ptr] = leaf
	}
}

func (os *obliviousStore) Commit(ctx context.Context, version uint64) error {
	err := os.base.Commit(ctx, version, os.Stash, os.assignments)

	os.Stash = nil
	os.Count = 0
	os.assignments = nil

	return err
}

func (os *obliviousStore) Rollback(ctx context.Context) {
	os.base.Rollback(ctx)

	os.Stash = nil
	os.Count = 0
	os.assignments = nil
}

type localOblivious struct {
	db *sql.DB

	tx      *sql.Tx
	version uint64
}

// NewLocalOblivious returns an implementation of the ObliviousStorage
// interface, used for storing temporary ORAM data, that's backed by an on-disk
// database at `loc`.
func NewLocalOblivious(loc string) (ObliviousStorage, error) {
	if err := os.MkdirAll(path.Dir(loc), 0744); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", loc)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS stash (ptr integer, val bytea, version integer, UNIQUE(ptr, version))")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS position (ptr integer, leaf integer, version integer, UNIQUE(ptr, version))")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS assignments (version integer not null primary key, data bytea)")
	if err != nil {
		return nil, err
	}

	return &localOblivious{db: db}, nil
}

func (lo *localOblivious) Start(ctx context.Context, version uint64) (map[uint64][]byte, uint64, error) {
	tx, err := lo.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, 0, err
	}

	stash, count, err := lo.initTx(ctx, version, tx)
	if err != nil {
		tx.Rollback()
		return nil, 0, err
	}

	lo.tx = tx
	lo.version = version
	return stash, count, nil
}

func (lo *localOblivious) initTx(ctx context.Context, version uint64, tx *sql.Tx) (map[uint64][]byte, uint64, error) {
	// Read the stash into memory.
	stash := make(map[uint64][]byte)

	rows, err := tx.QueryContext(ctx, "SELECT ptr, val FROM stash WHERE version = ?", version)
	if err != nil {
		return nil, 0, err
	}
	for rows.Next() {
		var (
			ptr uint64
			val []byte
		)
		if err := rows.Scan(&ptr, &val); err != nil {
			rows.Close()
			return nil, 0, err
		}
		stash[ptr] = val
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, 0, err
	}
	rows.Close()

	// Keep the stash clean by deleting entries that don't correspond to the
	// current version.
	_, err = tx.ExecContext(ctx, "DELETE FROM stash WHERE version != ?", version)
	if err != nil {
		return nil, 0, err
	}

	// Compute the count of leaves stored.
	var count *uint64
	err = tx.QueryRowContext(ctx, "SELECT MAX(ptr) FROM position WHERE version <= ?", version).Scan(&count)
	if err != nil {
		return nil, 0, err
	} else if count == nil {
		count = new(uint64)
	} else {
		*count += 1
	}

	// Keep the positions map clean by removing overlapping assignments and
	// removing future, failed versions.
	if err := lo.deleteOverlapping(ctx, version, tx); err != nil {
		return nil, 0, err
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM position WHERE version > ?", version)
	if err != nil {
		return nil, 0, err
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM assignments")
	if err != nil {
		return nil, 0, err
	}

	return stash, *count, nil
}

// deleteOverlapping deletes previous versions of assignments that were made
// while the current version was being created. That is, it makes sure there are
// no more rows with the same `ptr` value.
func (lo *localOblivious) deleteOverlapping(ctx context.Context, version uint64, tx *sql.Tx) error {
	var data []byte
	err := tx.QueryRowContext(ctx, "SELECT data FROM assignments WHERE version = ?", version).Scan(&data)
	if err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		return err
	}

	var ptrs []uint64
	if err := json.Unmarshal(data, &ptrs); err != nil {
		return err
	}
	ptrStrs := make([]string, 0, len(ptrs))
	for _, ptr := range ptrs {
		ptrStrs = append(ptrStrs, fmt.Sprint(ptr))
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM position WHERE ptr IN ("+strings.Join(ptrStrs, ",")+") AND version != ?", version)
	if err != nil {
		return err
	}

	return nil
}

func (lo *localOblivious) Lookup(ctx context.Context, ptrs []uint64) (map[uint64]uint64, error) {
	ptrStrs := make([]string, 0, len(ptrs))
	for _, ptr := range ptrs {
		ptrStrs = append(ptrStrs, fmt.Sprint(ptr))
	}
	out := make(map[uint64]uint64)

	rows, err := lo.tx.QueryContext(ctx, "SELECT ptr, leaf FROM position WHERE ptr IN ("+strings.Join(ptrStrs, ",")+")")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ptr, leaf uint64
		if err := rows.Scan(&ptr, &leaf); err != nil {
			rows.Close()
			return nil, err
		} else if _, ok := out[ptr]; ok {
			return nil, fmt.Errorf("oblivious: same pointer present multiple times in query results")
		}
		out[ptr] = leaf
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	return out, nil
}

func (lo *localOblivious) Commit(ctx context.Context, version uint64, stash map[uint64][]byte, assignments map[uint64]uint64) error {
	if version < lo.version {
		return fmt.Errorf("oblivious: cannot commit at lower version number than before")
	} else if version == lo.version {
		lo.Rollback(ctx)
		return nil
	}

	for ptr, val := range stash {
		_, err := lo.tx.ExecContext(ctx, "INSERT INTO stash (ptr, val, version) VALUES (?, ?, ?)", ptr, val, version)
		if err != nil {
			return err
		}
	}

	for ptr, leaf := range assignments {
		_, err := lo.tx.ExecContext(ctx, "INSERT INTO position (ptr, leaf, version) VALUES (?, ?, ?)", ptr, leaf, version)
		if err != nil {
			return err
		}
	}

	ptrs := make([]uint64, 0, len(assignments))
	for ptr, _ := range assignments {
		ptrs = append(ptrs, ptr)
	}
	data, err := json.Marshal(ptrs)
	if err != nil {
		return err
	}
	_, err = lo.tx.ExecContext(ctx, "INSERT INTO assignments (version, data) VALUES (?, ?)", version, data)
	if err != nil {
		return err
	}

	if err := lo.tx.Commit(); err != nil {
		return err
	}
	lo.tx = nil
	lo.version = 0
	return nil
}

func (lo *localOblivious) Rollback(ctx context.Context) {
	if lo.tx == nil {
		return
	}
	lo.tx.Rollback()
	lo.tx = nil
	lo.version = 0
}
