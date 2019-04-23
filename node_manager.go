package utahfs

import (
	"encoding/gob"
	"io"
	"os"
	"time"

	"github.com/hashicorp/golang-lru"
	"github.com/jacobsa/fuse/fuseops"
)

type node struct {
	bfs *BlockFilesystem

	self *BlockFile
	data *BlockFile

	Attrs    fuseops.InodeAttributes
	Children map[string]fuseops.InodeID
	Data     uint32
}

func (nd *node) open(create bool) error {
	if nd.data != nil {
		return nil
	} else if nd.Data == nilPtr {
		if create {
			ptr, bf, err := nd.bfs.Create()
			if err != nil {
				return err
			}
			nd.Data, nd.data = ptr, bf
			return nil
		}
		return io.EOF
	}

	bf, err := nd.bfs.Open(nd.Data)
	if err != nil {
		return err
	}
	nd.data = bf
	nd.data.size = int64(nd.Attrs.Size)
	return nil
}

func (nd *node) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= int64(nd.Attrs.Size) {
		return 0, io.EOF
	} else if err := nd.open(false); err != nil {
		return 0, err
	}

	if _, err := nd.data.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	return nd.data.Read(p)
}

func (nd *node) ReadAll() ([]byte, error) {
	if err := nd.open(false); err != nil {
		return nil, err
	} else if _, err := nd.data.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	acc := make([]byte, 0)
	for {
		buff := make([]byte, 1024)
		n, err := nd.ReadAt(buff, int64(len(acc)))
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		acc = append(acc, buff[:n]...)
	}
	return acc, nil
}

func (nd *node) WriteAt(p []byte, offset int64) (int, error) {
	if err := nd.open(true); err != nil {
		return 0, err
	}
	defer func() {
		nd.Attrs.Size = uint64(nd.data.size)
	}()

	// If we're trying to write past the end of the file, pad with null bytes.
	if uint64(offset) > nd.Attrs.Size {
		if _, err := nd.data.Seek(0, io.SeekEnd); err != nil {
			return 0, err
		}
		n, err := nd.data.Write(make([]byte, uint64(offset)-nd.Attrs.Size))
		if err != nil {
			return n, err
		}
	}

	if _, err := nd.data.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	n, err := nd.data.Write(p)
	if err != nil {
		return n, err
	}

	return n, nil
}

func (nd *node) Truncate(size int64) error {
	if err := nd.open(true); err != nil {
		return err
	}
	defer func() {
		nd.Attrs.Size = uint64(nd.data.size)
	}()

	return nd.data.Truncate(size)
}

func (nd *node) Equals(other *node) bool {
	if nd == nil && other == nil {
		return true
	} else if nd == nil || other == nil {
		return false
	}
	return nd.self.start == other.self.start
}

// Persist writes the node to storage, to capture any changes to this struct's
// fields.
func (nd *node) Persist() error {
	if _, err := nd.self.Seek(0, io.SeekStart); err != nil {
		return err
	} else if err := gob.NewEncoder(nd.self).Encode(nd); err != nil {
		return err
	}
	pos, err := nd.self.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	} else if err := nd.self.Truncate(pos); err != nil {
		return err
	}
	return nil
}

// nodeManager implements the creation, opening, and deletion of filesystem
// nodes over a block filesystem.
//
// The prefix of each block file is a gob-encoded structure containing metadata,
// links to children, and the rest is the node's raw data.
type nodeManager struct {
	store AppStorage
	bfs   *BlockFilesystem
	cache *lru.Cache
}

func newNodeManager(store AppStorage, bfs *BlockFilesystem, cacheSize int) (*nodeManager, error) {
	cache, err := lru.New(cacheSize)
	if err != nil {
		return nil, err
	}
	return &nodeManager{
		store: store,
		bfs:   bfs,
		cache: cache,
	}, nil
}

func (nm *nodeManager) Start() error  { return nm.store.Start() }
func (nm *nodeManager) Commit() error { return nm.store.Commit() }
func (nm *nodeManager) Rollback()     { nm.store.Rollback() }

func (nm *nodeManager) State() (*State, error) { return nm.store.State() }

func (nm *nodeManager) Create(mode os.FileMode, uid, gid uint32) (uint32, error) {
	now := time.Now()
	nd := node{
		Attrs: fuseops.InodeAttributes{
			Size: 0,

			Nlink: 1,

			Mode: mode,

			Atime:  now,
			Mtime:  now,
			Ctime:  now,
			Crtime: now,

			Uid: uid,
			Gid: gid,
		},
		Children: nil,
		Data:     nilPtr,
	}
	if nd.Attrs.Mode.IsDir() {
		nd.Children = make(map[string]fuseops.InodeID)
	}

	ptr, bf, err := nm.bfs.Create()
	if err != nil {
		return nilPtr, err
	} else if err := gob.NewEncoder(bf).Encode(nd); err != nil {
		return nilPtr, err
	}
	return ptr, nil
}

func (nm *nodeManager) Open(ptr uint32) (*node, error) {
	if nd, ok := nm.cache.Get(ptr); ok {
		return nd.(*node), nil
	}

	bf, err := nm.bfs.Open(ptr)
	if err != nil {
		return nil, err
	}
	nd := &node{}
	if err := gob.NewDecoder(bf).Decode(nd); err != nil {
		return nil, err
	}
	nd.bfs = nm.bfs
	nd.self = bf

	nm.cache.Add(ptr, nd)
	return nd, nil
}

func (nm *nodeManager) Unlink(ptr uint32) error {
	nd, err := nm.Open(ptr)
	if err != nil {
		return err
	} else if err := nm.bfs.Unlink(uint32(ptr)); err != nil {
		return err
	} else if nd.Data != nilPtr {
		return nm.bfs.Unlink(nd.Data)
	}
	return nil
}
