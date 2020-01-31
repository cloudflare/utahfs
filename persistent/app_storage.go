package persistent

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

var AppStorageCommits = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "app_storage_commits",
	Help: "The number of successful app storage transactions committed.",
})

// State contains all of the shared global state of a deployment.
type State struct {
	// RootPtr points to the root inode of the filesystem.
	RootPtr uint64

	// Blocks that were previously allocated but are now un-used are kept in a
	// linked list. TrashPtr points to the head of this list.
	TrashPtr uint64
	// NextPtr will be the pointer of the next block which is allocated.
	NextPtr uint64
}

func NewState() *State {
	return &State{
		RootPtr: nilPtr,

		TrashPtr: nilPtr,
		NextPtr:  0,
	}
}

func (s *State) Clone() *State {
	return &State{
		RootPtr: s.RootPtr,

		TrashPtr: s.TrashPtr,
		NextPtr:  s.NextPtr,
	}
}

// AppStorage is an extension of the BlockStorage interface that provides shared
// state.
type AppStorage struct {
	base BlockStorage

	active          bool
	original, state *State
}

func NewAppStorage(base BlockStorage) *AppStorage {
	return &AppStorage{base: base}
}

func (as *AppStorage) Start(ctx context.Context) error {
	if as.active {
		return fmt.Errorf("app: transaction already started")
	}

	if _, err := as.base.Start(ctx, nil); err != nil {
		return err
	}
	as.active = true

	return nil
}

// State returns a struct of shared global state. Consumers may modify the
// returned struct, and these modifications will be persisted after Commit is
// called.
func (as *AppStorage) State(ctx context.Context) (*State, error) {
	if !as.active {
		return nil, fmt.Errorf("app: transaction not active")
	} else if as.state != nil {
		return as.state, nil
	}

	raw, err := as.base.Get(ctx, 0)
	if err == ErrObjectNotFound {
		as.original, as.state = NewState(), NewState()
	} else if err != nil {
		return nil, err
	} else {
		state := &State{}
		if err := gob.NewDecoder(bytes.NewBuffer(raw)).Decode(state); err != nil {
			return nil, err
		}
		as.original, as.state = state, state.Clone()
	}

	return as.state, nil
}

func (as *AppStorage) Get(ctx context.Context, ptr uint64) ([]byte, error) {
	if !as.active {
		return nil, fmt.Errorf("app: transaction not active")
	}
	return as.base.Get(ctx, ptr+1)
}

func (as *AppStorage) GetMany(ctx context.Context, ptrs []uint64) (map[uint64][]byte, error) {
	if !as.active {
		return nil, fmt.Errorf("app: transaction not active")
	}

	corrected := make([]uint64, 0, len(ptrs))
	for _, ptr := range ptrs {
		corrected = append(corrected, ptr+1)
	}

	data, err := as.base.GetMany(ctx, corrected)
	if err != nil {
		return nil, err
	}

	out := make(map[uint64][]byte)
	for ptr, val := range data {
		out[ptr-1] = val
	}
	return out, nil
}

func (as *AppStorage) Set(ctx context.Context, ptr uint64, data []byte, dt DataType) error {
	if !as.active {
		return fmt.Errorf("app: transaction not active")
	}
	return as.base.Set(ctx, ptr+1, data, dt)
}

func (as *AppStorage) Commit(ctx context.Context) error {
	if !as.active {
		return fmt.Errorf("app: transaction not active")
	}

	if as.original != nil && *as.original != *as.state {
		buff := &bytes.Buffer{}
		if err := gob.NewEncoder(buff).Encode(as.state); err != nil {
			return err
		} else if err := as.base.Set(ctx, 0, buff.Bytes(), Metadata); err != nil {
			return err
		}
	}
	if err := as.base.Commit(ctx); err != nil {
		return err
	}
	as.active = false
	as.original, as.state = nil, nil

	AppStorageCommits.Inc()
	return nil
}

func (as *AppStorage) Rollback(ctx context.Context) {
	as.base.Rollback(ctx)
	as.active = false
	as.original, as.state = nil, nil
}
