package utahfs

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
)

// State contains all of the shared global state of a deployment.
type State struct {
	// RootPtr points to the root inode of the filesystem.
	RootPtr uint32

	// Blocks that were previously allocated but are now un-used are kept in a
	// linked list. TrashPtr points to the head of this list.
	TrashPtr uint32
	// NextPtr will be the pointer of the next block which is allocated.
	NextPtr uint32
}

func newState() *State {
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

type changes struct {
	Original *State

	State  *State
	Writes map[string][]byte
}

func newChanges(state *State) *changes {
	return &changes{
		Original: state.Clone(),

		State:  state,
		Writes: make(map[string][]byte),
	}
}

// AppStorage is an extension of the ReliableStorage interface that provides
// shared state.
type AppStorage struct {
	store   ReliableStorage
	pending *changes
}

func NewAppStorage(store ReliableStorage) *AppStorage {
	return &AppStorage{store: store}
}

func (as *AppStorage) Start(ctx context.Context) error {
	if as.pending != nil {
		return fmt.Errorf("app: transaction already started")
	}

	if err := as.store.Start(ctx); err != nil {
		return err
	}
	raw, err := as.store.Get(ctx, "state")
	if err == ErrObjectNotFound {
		as.pending = newChanges(newState())
		return nil
	} else if err != nil {
		return err
	}

	state := &State{}
	if err := gob.NewDecoder(bytes.NewBuffer(raw)).Decode(state); err != nil {
		return err
	}
	as.pending = newChanges(state)

	return nil
}

// State returns a struct of shared global state. Consumers may modify the
// returned struct, and these modifications will be persisted after Commit is
// called.
func (as *AppStorage) State() (*State, error) {
	if as.pending == nil {
		return nil, fmt.Errorf("app: transaction not active")
	}
	return as.pending.State, nil
}

func (as *AppStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if as.pending == nil {
		return nil, fmt.Errorf("app: transaction not active")
	}
	key = "d" + key

	if data, ok := as.pending.Writes[key]; ok {
		if data != nil {
			return dup(data), nil
		}
		return nil, ErrObjectNotFound
	}
	return as.store.Get(ctx, key)
}

func (as *AppStorage) Set(ctx context.Context, key string, data []byte) error {
	if as.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}
	key = "d" + key

	as.pending.Writes[key] = dup(data)
	return nil
}

func (as *AppStorage) Delete(ctx context.Context, key string) error {
	if as.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}
	key = "d" + key

	as.pending.Writes[key] = nil
	return nil
}

// Commit persists any changes made to the backend.
func (as *AppStorage) Commit(ctx context.Context) error {
	if as.pending == nil {
		return fmt.Errorf("app: transaction not active")
	}

	if *as.pending.Original != *as.pending.State {
		buff := &bytes.Buffer{}
		if err := gob.NewEncoder(buff).Encode(as.pending.State); err != nil {
			return err
		}
		as.pending.Writes["state"] = buff.Bytes()
	}
	if err := as.store.Commit(ctx, as.pending.Writes); err != nil {
		return err
	}
	as.pending = nil

	return nil
}

// Rollback discards all changes made in this transaction.
func (as *AppStorage) Rollback(ctx context.Context) {
	as.store.Commit(ctx, nil)
	as.pending = nil
}
