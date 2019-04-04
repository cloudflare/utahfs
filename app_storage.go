package utahfs

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
)

// AppStorage is an extension of the ObjectStorage interface that provides
// shared state and atomic transactions.
type AppStorage interface {
	// Start begins a new transaction. None of the methods below will work until
	// this is called, and will stop working again after Commit or Rollback is
	// called.
	//
	// Transactions are isolated and atomic.
	Start() error

	// State returns a map of shared global state. Consumers may modify the
	// returned map, and these modifications will be persisted after Commit is
	// called.
	State() (map[string]interface{}, error)

	ObjectStorage

	// Commit persists any changes made to the backend.
	Commit() error
	// Rollback discards all changes made in this transaction.
	Rollback()
}

type wal struct {
	State   map[string]interface{}
	Writes  map[string][]byte
	Deletes map[string]struct{}
}

func newWAL(state map[string]interface{}) *wal {
	return &wal{
		State:   state,
		Writes:  make(map[string][]byte),
		Deletes: make(map[string]struct{}),
	}
}

type localWAL struct {
	walFile string
	store   ObjectStorage

	started bool
	curr    *wal
}

// NewLocalWAL returns an implementation of the AppStorage interface which works
// by first writing all changes to a file at `walFile` before trying to move any
// to the object storage provider `store`.
func NewLocalWAL(walFile string, store ObjectStorage) AppStorage {
	return &localWAL{
		walFile: walFile,
		store:   store,
	}
}

func (lw *localWAL) Start() error {
	if lw.started {
		return fmt.Errorf("wal: transaction already started")
	}

	raw, err := ioutil.ReadFile(lw.walFile)
	if os.IsNotExist(err) {
		return lw.start()
	} else if err != nil {
		return err
	} // else, a previous WAL exists. Try to parse and re-apply it.

	lw.curr = &wal{}
	if err := gob.NewDecoder(bytes.NewBuffer(raw)).Decode(lw.curr); err != nil {
		if err := os.Remove(lw.walFile); err != nil {
			return err
		}
		return lw.start()
	}

	if err := lw.commit(); err != nil {
		return err
	}
	return lw.start()
}

func (lw *localWAL) start() error {
	raw, err := lw.store.Get("state")
	if err == ErrObjectNotFound {
		lw.started, lw.curr = true, newWAL(make(map[string]interface{}))
		return nil
	} else if err != nil {
		return err
	}

	state := make(map[string]interface{})
	if err := gob.NewDecoder(bytes.NewBuffer(raw)).Decode(&state); err != nil {
		return err
	}
	lw.started, lw.curr = true, newWAL(state)

	return nil
}

func (lw *localWAL) State() (map[string]interface{}, error) {
	if !lw.started {
		return nil, fmt.Errorf("wal: transaction not active")
	}
	return lw.curr.State, nil
}

func (lw *localWAL) Get(key string) ([]byte, error) {
	if !lw.started {
		return nil, fmt.Errorf("wal: transaction not active")
	}
	if data, ok := lw.curr.Writes[key]; ok {
		return dup(data), nil
	} else if _, ok := lw.curr.Deletes[key]; ok {
		return nil, ErrObjectNotFound
	}
	return lw.store.Get("d" + key)
}

func (lw *localWAL) Set(key string, data []byte) error {
	if !lw.started {
		return fmt.Errorf("wal: transaction not active")
	}
	lw.curr.Writes[key] = dup(data)
	delete(lw.curr.Deletes, key)
	return nil
}

func (lw *localWAL) Delete(key string) error {
	if !lw.started {
		return fmt.Errorf("wal: transaction not active")
	}
	delete(lw.curr.Writes, key)
	lw.curr.Deletes[key] = struct{}{}
	return nil
}

func (lw *localWAL) Commit() error {
	if !lw.started {
		return fmt.Errorf("wal: transaction not active")
	}
	fh, err := os.Create(lw.walFile)
	if err != nil {
		return err
	} else if err := gob.NewEncoder(fh).Encode(lw.curr); err != nil {
		return err
	} else if err := fh.Sync(); err != nil {
		return err
	} else if err := fh.Close(); err != nil {
		return err
	}

	return lw.commit()
}

func (lw *localWAL) commit() error {
	state := &bytes.Buffer{}
	if err := gob.NewEncoder(state).Encode(lw.curr.State); err != nil {
		return err
	} else if err := lw.store.Set("state", state.Bytes()); err != nil {
		return err
	}

	for key, val := range lw.curr.Writes {
		if err := lw.store.Set("d"+key, val); err != nil {
			return err
		}
	}

	for key, _ := range lw.curr.Deletes {
		if err := lw.store.Delete("d" + key); err != nil {
			return err
		}
	}

	if err := os.Remove(lw.walFile); err != nil {
		return err
	}
	lw.started, lw.curr = false, nil
	return nil
}

func (lw *localWAL) Rollback() {
	lw.started, lw.curr = false, nil
}
