package utahfs

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Note: This implementation is largely based on
// github.com/GoogleCloudPlatform/gcsfuse. If some decision seems weird, it
// might be justified in that codebase.

func commit(nm *nodeManager, nds ...*node) error {
	for _, nd := range nds {
		if err := nd.Persist(); err != nil {
			log.Println(err)
			return fuse.EIO
		}
	}
	if err := nm.Commit(); err != nil {
		log.Println(err)
		return fuse.EIO
	}
	return nil
}

type filesystem struct {
	fuseutil.NotImplementedFileSystem

	nm      *nodeManager
	rootPtr uint32
	refs    map[fuseops.InodeID]int

	mu sync.Mutex
}

// NewFilesystem returns a FUSE binding that internally stores data in a
// block-based filesystem.
func NewFilesystem(store AppStorage, bfs *BlockFilesystem) (fuseutil.FileSystem, error) {
	nm, err := newNodeManager(store, bfs, 128)
	if err != nil {
		return nil, err
	} else if err := nm.Start(); err != nil {
		return nil, err
	}
	defer nm.Rollback()

	state, err := nm.State()
	if err != nil {
		return nil, err
	} else if state.RootPtr == nilPtr {
		rootPtr, err := nm.Create(os.ModeDir|0777, 0, 0)
		if err != nil {
			return nil, err
		}
		state.RootPtr = rootPtr
		if err := nm.Commit(); err != nil {
			return nil, err
		}
	}

	return &filesystem{
		nm:      nm,
		rootPtr: state.RootPtr,
		refs:    make(map[fuseops.InodeID]int),
	}, nil
}

func (fs *filesystem) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	// See gcfuse for justification.
	op.BlockSize = 1 << 17
	op.Blocks = 1 << 33
	op.BlocksFree = op.Blocks
	op.BlocksAvailable = op.Blocks

	op.Inodes = 1 << 50
	op.InodesFree = op.Inodes

	op.IoSize = 1 << 20

	return nil
}

func (fs *filesystem) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	defer fs.synchronize()()

	nd, err := fs.nm.Open(fs.ptr(op.Parent))
	if err != nil {
		return err
	} else if nd.Children == nil {
		return fuse.ENOTDIR
	}
	childId, ok := nd.Children[op.Name]
	if !ok {
		return fuse.ENOENT
	}
	child, err := fs.nm.Open(fs.ptr(childId))
	if err != nil {
		return err
	}

	op.Entry.Child = childId
	op.Entry.Attributes = child.Attrs
	op.Entry.AttributesExpiration = fs.expiration()

	return nil
}

func (fs *filesystem) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	defer fs.synchronize()()

	nd, err := fs.nm.Open(fs.ptr(op.Inode))
	if err != nil {
		return err
	}
	op.Attributes = nd.Attrs
	op.AttributesExpiration = fs.expiration()

	return nil
}

func (fs *filesystem) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	defer fs.synchronize()()

	nd, err := fs.nm.Open(fs.ptr(op.Inode))
	if err != nil {
		return err
	} else if !nd.Attrs.Mode.IsRegular() {
		op.Attributes = nd.Attrs
		op.AttributesExpiration = fs.expiration()
		return nil
	}

	if op.Size != nil {
		if err := nd.Truncate(int64(*op.Size)); err != nil {
			return err
		}
	}
	if op.Mtime != nil {
		nd.Attrs.Mtime = *op.Mtime
	}
	// Silently ignore updates to mode and atime.

	return commit(fs.nm, nd)
}

func (fs *filesystem) ForgetInode(ctx context.Context, op *fuseops.ForgetInodeOp) error {
	defer fs.synchronize()()

	n, m := fs.refs[op.Inode], int(op.N)
	if n < m {
		panic(fmt.Sprintf("n is greater than lookup count: %v vs %v", n, m))
	} else if n > m {
		fs.refs[op.Inode] = n - m
		return nil
	} // else n == m

	delete(fs.refs, op.Inode)

	nd, err := fs.nm.Open(fs.ptr(op.Inode))
	if err != nil {
		return err
	} else if nd.Attrs.Nlink > 0 {
		return nil
	} else if err := fs.nm.Unlink(fs.ptr(op.Inode)); err != nil {
		return err
	}

	return commit(fs.nm)
}

func (fs *filesystem) synchronize() func() {
	fs.mu.Lock()
	if err := fs.nm.Start(); err != nil {
		log.Fatal(err)
	}
	return func() {
		if r := recover(); r != nil {
			log.Println(r)
			log.Println(string(debug.Stack()))
			panic(r)
		}
		fs.nm.Rollback()
		fs.mu.Unlock()
	}
}

func (fs *filesystem) ptr(id fuseops.InodeID) uint32 {
	return uint32(id) + fs.rootPtr - 1
}

func (fs *filesystem) inode(ptr uint32) fuseops.InodeID {
	return fuseops.InodeID(ptr - fs.rootPtr + 1)
}

func (fs *filesystem) expiration() time.Time {
	return time.Now().Add(time.Minute)
}
