package utahfs

import (
	"context"
	"encoding/gob"
	"io"
	"os"
	"time"

	"github.com/cloudflare/utahfs/cache"
	"github.com/cloudflare/utahfs/persistent"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type node struct {
	bfs *BlockFilesystem
	ctx context.Context

	self *BlockFile
	data *BlockFile

	Attrs    fuseops.InodeAttributes
	Children map[string]fuseops.InodeID
	Data     uint64
}

func (nd *node) open(create bool) error {
	if nd.data != nil {
		return nil
	} else if nd.Data == nilPtr {
		if create {
			ptr, bf, err := nd.bfs.Create(nd.ctx, persistent.Content)
			if err != nil {
				return err
			}
			nd.Data, nd.data = ptr, bf
			return nil
		}
		return io.EOF
	}

	bf, err := nd.bfs.Open(nd.ctx, nd.Data, persistent.Content)
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

	if nd.data.pos != offset {
		if _, err := nd.data.Seek(offset, io.SeekStart); err != nil {
			return 0, err
		}
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

	if uint64(size) > nd.Attrs.Size {
		_, err := nd.data.Write(make([]byte, uint64(size)-nd.Attrs.Size))
		return err
	}
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

func (nd *node) Type() fuseutil.DirentType {
	if nd.Attrs.Mode.IsRegular() {
		return fuseutil.DT_File
	} else if nd.Attrs.Mode.IsDir() {
		return fuseutil.DT_Directory
	}
	return fuseutil.DT_Unknown
}

// Persist writes the node to storage, to capture any changes to this struct's
// fields.
func (nd *node) Persist() error {
	uid, gid := nd.Attrs.Uid, nd.Attrs.Gid
	nd.Attrs.Uid, nd.Attrs.Gid = 0, 0
	defer func() {
		nd.Attrs.Uid, nd.Attrs.Gid = uid, gid
	}()

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
	bfs   *BlockFilesystem
	cache *cache.Cache

	uid, gid uint32
}

func newNodeManager(bfs *BlockFilesystem, cacheSize int, uid, gid uint32) *nodeManager {
	return &nodeManager{
		bfs:   bfs,
		cache: cache.New(30*time.Second, 5*time.Second, cacheSize),

		uid: uid,
		gid: gid,
	}
}

func (nm *nodeManager) Start(ctx context.Context) error  { return nm.bfs.store.Start(ctx) }
func (nm *nodeManager) Commit(ctx context.Context) error { return nm.bfs.store.Commit(ctx) }
func (nm *nodeManager) Rollback(ctx context.Context)     { nm.bfs.store.Rollback(ctx) }

func (nm *nodeManager) State(ctx context.Context) (*persistent.State, error) {
	return nm.bfs.store.State(ctx)
}

func (nm *nodeManager) Create(ctx context.Context, mode os.FileMode) (uint64, error) {
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
		},
		Children: nil,
		Data:     nilPtr,
	}
	if nd.Attrs.Mode.IsDir() {
		nd.Children = make(map[string]fuseops.InodeID)
	}

	ptr, bf, err := nm.bfs.Create(ctx, persistent.Metadata)
	if err != nil {
		return nilPtr, err
	} else if err := gob.NewEncoder(bf).Encode(nd); err != nil {
		return nilPtr, err
	}
	return ptr, nil
}

func (nm *nodeManager) Open(ctx context.Context, ptr uint64) (*node, error) {
	if val, ok := nm.cache.Get(ptr); ok {
		nd := val.(*node)
		nd.ctx, nd.self.ctx = ctx, ctx
		if nd.data != nil {
			nd.data.ctx = ctx
		}
		return nd, nil
	}

	bf, err := nm.bfs.Open(ctx, ptr, persistent.Metadata)
	if err != nil {
		return nil, err
	}
	nd := &node{}
	if err := gob.NewDecoder(bf).Decode(nd); err != nil {
		return nil, err
	}
	nd.ctx = ctx
	nd.bfs = nm.bfs
	nd.self = bf
	nd.Attrs.Uid = nm.uid
	nd.Attrs.Gid = nm.gid

	nm.cache.Set(ptr, nd, cache.DefaultExpiration)
	return nd, nil
}

func (nm *nodeManager) Unlink(ctx context.Context, ptr uint64) error {
	nd, err := nm.Open(ctx, ptr)
	if err != nil {
		return err
	} else if err := nm.bfs.Unlink(ctx, ptr); err != nil {
		return err
	} else if nd.Data != nilPtr {
		return nm.bfs.Unlink(ctx, nd.Data)
	}
	return nil
}

func (nm *nodeManager) Forget(nd *node) {
	nm.cache.Delete(nd.self.start)
}
