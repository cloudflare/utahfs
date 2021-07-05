package utahfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Note: This implementation is largely based on
// github.com/GoogleCloudPlatform/gcsfuse. If some decision seems weird, it
// might be justified in that codebase.

// Note: If a function modifies a node, that node must immediately be passed
// into commit() to be persisted, or forgotten if there's an error persisting.
// If there's an error path that could cause the function to return before
// getting to commit(), then the function must manually forget the node.

type dirHandle struct {
	inode    fuseops.InodeID
	entries  []fuseutil.Dirent
	children map[string]fuseops.ChildInodeEntry
}

func now() time.Time {
	return time.Now().Round(time.Second)
}

func myUserAndGroup() (uint32, uint32, error) {
	user, err := user.Current()
	if err != nil {
		return 0, 0, err
	}

	uid, err := strconv.ParseUint(user.Uid, 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse uid (%s): %v", user.Uid, err)
	}
	gid, err := strconv.ParseUint(user.Gid, 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse gid (%s): %v", user.Gid, err)
	}

	return uint32(uid), uint32(gid), nil
}

func commit(ctx context.Context, nm *nodeManager, nds ...*node) error {
	for _, nd := range nds {
		if err := nd.Persist(); err != nil {
			for _, nd := range nds {
				nm.Forget(nd)
			}
			log.Println(err)
			return fuse.EIO
		}
	}
	if err := nm.Commit(ctx); err != nil {
		for _, nd := range nds {
			nm.Forget(nd)
		}
		log.Println(err)
		return fuse.EIO
	}
	return nil
}

type filesystem struct {
	fuseutil.NotImplementedFileSystem

	nm      *nodeManager
	rootPtr uint64

	nextHandleID fuseops.HandleID
	dirHandles   map[fuseops.HandleID]dirHandle
	fileHandles  map[fuseops.HandleID]struct{}

	mu sync.Mutex
}

// NewFilesystem returns a FUSE binding that internally stores data in a
// block-based filesystem.
func NewFilesystem(bfs *BlockFilesystem) (fuseutil.FileSystem, error) {
	ctx := context.Background()

	uid, gid, err := myUserAndGroup()
	if err != nil {
		return nil, err
	}
	nm := newNodeManager(bfs, 128, uid, gid)
	if err := nm.Start(ctx); err != nil {
		return nil, err
	}
	defer nm.Rollback(ctx)

	state, err := nm.State(ctx)
	if err != nil {
		return nil, err
	} else if state.RootPtr == nilPtr {
		rootPtr, err := nm.Create(ctx, os.ModeDir|0777)
		if err != nil {
			return nil, err
		}
		state.RootPtr = rootPtr
		if err := nm.Commit(ctx); err != nil {
			return nil, err
		}
	}

	return &filesystem{
		nm:      nm,
		rootPtr: state.RootPtr,

		dirHandles:  make(map[fuseops.HandleID]dirHandle),
		fileHandles: make(map[fuseops.HandleID]struct{}),
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
	// When the filesystem is reading a directory, it issues LookUpInode ops for
	// every entry it reads. Since we already looked up every node when we were
	// first opening the directory, looking them all up again would be wasteful.
	// Try to answer the query by getting this data from the open handle.
	fs.mu.Lock()
	for _, handle := range fs.dirHandles {
		if handle.inode != op.Parent {
			continue
		}
		child, ok := handle.children[op.Name]
		if !ok {
			fs.mu.Unlock()
			return fuse.ENOENT
		}
		op.Entry = child
		fs.mu.Unlock()
		return nil
	}
	fs.mu.Unlock()

	// That failed. Answer the query normally: by starting a transaction and
	// getting the data we need from the backend.
	defer fs.synchronize(ctx)()

	nd, err := fs.nm.Open(ctx, fs.ptr(op.Parent))
	if err != nil {
		return err
	} else if nd.Children == nil {
		return fuse.ENOTDIR
	}
	childID, ok := nd.Children[op.Name]
	if !ok {
		return fuse.ENOENT
	}
	child, err := fs.nm.Open(ctx, fs.ptr(childID))
	if err != nil {
		return err
	}

	op.Entry.Child = childID
	op.Entry.Attributes = child.Attrs
	op.Entry.AttributesExpiration = fs.expiration()
	op.Entry.EntryExpiration = fs.expiration()

	return nil
}

func (fs *filesystem) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	fs.mu.Lock()
	for _, handle := range fs.dirHandles {
		for _, nd := range handle.children {
			if nd.Child == op.Inode {
				op.Attributes = nd.Attributes
				op.AttributesExpiration = fs.expiration()
				fs.mu.Unlock()
				return nil
			}
		}
	}
	fs.mu.Unlock()
	defer fs.synchronize(ctx)()

	nd, err := fs.nm.Open(ctx, fs.ptr(op.Inode))
	if err != nil {
		return err
	}
	op.Attributes = nd.Attrs
	op.AttributesExpiration = fs.expiration()

	return nil
}

func (fs *filesystem) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	return fs.setInodeAttributes(ctx, op, false)
}

func (fs *filesystem) setInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp, archive bool) error {
	defer fs.synchronize(ctx)()

	nd, err := fs.nm.Open(ctx, fs.ptr(op.Inode))
	if err != nil {
		return err
	} else if !nd.Attrs.Mode.IsRegular() {
		op.Attributes = nd.Attrs
		op.AttributesExpiration = fs.expiration()
		return nil
	}

	if op.Size != nil {
		if *op.Size < nd.Attrs.Size && archive {
			return fmt.Errorf("utahfs: refusing to truncate archived file")
		} else if err := nd.Truncate(int64(*op.Size)); err != nil {
			return err
		}
	}
	if op.Mode != nil {
		nd.Attrs.Mode = *op.Mode
	}
	if op.Mtime != nil {
		nd.Attrs.Mtime = *op.Mtime
	}
	// Silently ignore updates to atime.

	if op.Size != nil || op.Mode != nil || op.Mtime != nil {
		nd.Attrs.Ctime = now()
	}

	return commit(ctx, fs.nm, nd)
}

func (fs *filesystem) ForgetInode(ctx context.Context, op *fuseops.ForgetInodeOp) error {
	return nil
}

func (fs *filesystem) MkDir(ctx context.Context, op *fuseops.MkDirOp) error {
	defer fs.synchronize(ctx)()

	parent, child, err := fs.mkNode(ctx, op.Parent, op.Name, op.Mode)
	if err != nil {
		return err
	}
	op.Entry.Child = parent.Children[op.Name]
	op.Entry.Attributes = child.Attrs
	op.Entry.AttributesExpiration = fs.expiration()
	op.Entry.EntryExpiration = fs.expiration()

	return commit(ctx, fs.nm, parent)
}

func (fs *filesystem) MkNode(ctx context.Context, op *fuseops.MkNodeOp) error {
	defer fs.synchronize(ctx)()

	parent, child, err := fs.mkNode(ctx, op.Parent, op.Name, op.Mode)
	if err != nil {
		return err
	}
	op.Entry.Child = parent.Children[op.Name]
	op.Entry.Attributes = child.Attrs
	op.Entry.AttributesExpiration = fs.expiration()
	op.Entry.EntryExpiration = fs.expiration()

	return commit(ctx, fs.nm, parent)
}

func (fs *filesystem) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	defer fs.synchronize(ctx)()

	parent, child, err := fs.mkNode(ctx, op.Parent, op.Name, op.Mode)
	if err != nil {
		return err
	}
	op.Entry.Child = parent.Children[op.Name]
	op.Entry.Attributes = child.Attrs
	op.Entry.AttributesExpiration = fs.expiration()
	op.Entry.EntryExpiration = fs.expiration()

	// Issue the next handle ID. It doesn't mean anything.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = struct{}{}
	op.Handle = handleID

	return commit(ctx, fs.nm, parent)
}

func (fs *filesystem) CreateSymlink(ctx context.Context, op *fuseops.CreateSymlinkOp) error {
	defer fs.synchronize(ctx)()

	parent, child, err := fs.mkNode(ctx, op.Parent, op.Name, os.ModeSymlink|0755)
	if err != nil {
		return err
	} else if _, err := child.WriteAt([]byte(op.Target), 0); err != nil {
		fs.nm.Forget(parent)
		fs.nm.Forget(child)
		return err
	}
	op.Entry.Child = parent.Children[op.Name]
	op.Entry.Attributes = child.Attrs
	op.Entry.AttributesExpiration = fs.expiration()
	op.Entry.EntryExpiration = fs.expiration()

	return commit(ctx, fs.nm, parent, child)
}

func (fs *filesystem) Rename(ctx context.Context, op *fuseops.RenameOp) error {
	return fs.rename(ctx, op, false)
}

func (fs *filesystem) rename(ctx context.Context, op *fuseops.RenameOp, archive bool) error {
	defer fs.synchronize(ctx)()

	if op.OldParent == op.NewParent && op.OldName == op.NewName {
		return nil
	}

	oldParent, err := fs.nm.Open(ctx, fs.ptr(op.OldParent))
	if err != nil {
		return err
	}
	id, ok := oldParent.Children[op.OldName]
	if !ok {
		return fuse.ENOENT
	}

	if op.NewParent == id {
		return fuse.EINVAL
	}
	newParent, err := fs.nm.Open(ctx, fs.ptr(op.NewParent))
	if err != nil {
		return err
	} else if _, ok := newParent.Children[op.NewName]; ok {
		if err := fs.rmNode(ctx, newParent, op.NewName, archive); err != nil {
			return err
		}
	}

	if op.OldParent == op.NewParent {
		delete(newParent.Children, op.OldName)
		newParent.Children[op.NewName] = id
	} else {
		delete(oldParent.Children, op.OldName)
		newParent.Children[op.NewName] = id
	}

	oldParent.Attrs.Mtime = now()
	oldParent.Attrs.Ctime = now()
	newParent.Attrs.Mtime = now()
	newParent.Attrs.Ctime = now()

	return commit(ctx, fs.nm, oldParent, newParent)
}

func (fs *filesystem) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	return fs.Unlink(ctx, &fuseops.UnlinkOp{op.Parent, op.Name, op.OpContext})
}

func (fs *filesystem) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	return fs.unlink(ctx, op, false)
}

func (fs *filesystem) unlink(ctx context.Context, op *fuseops.UnlinkOp, archive bool) error {
	defer fs.synchronize(ctx)()

	parent, err := fs.nm.Open(ctx, fs.ptr(op.Parent))
	if err != nil {
		return err
	} else if err := fs.rmNode(ctx, parent, op.Name, archive); err != nil {
		return err
	}

	return commit(ctx, fs.nm, parent)
}

func (fs *filesystem) OpenDir(ctx context.Context, op *fuseops.OpenDirOp) error {
	defer fs.synchronize(ctx)()

	nd, err := fs.nm.Open(ctx, fs.ptr(op.Inode))
	if err != nil {
		return err
	} else if !nd.Attrs.Mode.IsDir() {
		return fuse.ENOTDIR
	}

	// Alphabetize the entries in the directory, and convert them into a sorted
	// []fuseutil.Dirent, which is more easily serialized.
	names := make([]string, 0, len(nd.Children))
	for name, _ := range nd.Children {
		names = append(names, name)
	}
	sort.Strings(names)

	children := make(map[string]fuseops.ChildInodeEntry)
	entries := make([]fuseutil.Dirent, 0, len(nd.Children))
	for i, name := range names {
		childID := nd.Children[name]
		child, err := fs.nm.Open(ctx, fs.ptr(childID))
		if err != nil {
			return fmt.Errorf("failed to open inode for child: %v", err)
		}

		children[name] = fuseops.ChildInodeEntry{
			Child:                childID,
			Attributes:           child.Attrs,
			AttributesExpiration: fs.expiration(),
		}
		entries = append(entries, fuseutil.Dirent{
			fuseops.DirOffset(i + 1), childID, name, child.Type(),
		})
	}

	// Attach slice of entries to the next handle id and return.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.dirHandles[handleID] = dirHandle{
		inode:    op.Inode,
		children: children,
		entries:  entries,
	}
	op.Handle = handleID

	return nil
}

func (fs *filesystem) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) error {
	defer fs.synchronize(ctx)()

	handle, ok := fs.dirHandles[op.Handle]
	if !ok {
		return fmt.Errorf("failed to release unknown handle")
	}
	idx := int(op.Offset)
	if idx > len(handle.entries) {
		return fuse.EINVAL
	}

	for i := idx; i < len(handle.entries); i++ {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], handle.entries[i])
		if n == 0 {
			break
		}
		op.BytesRead += n
	}

	return nil
}

func (fs *filesystem) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) error {
	defer fs.synchronize(ctx)()

	_, ok := fs.dirHandles[op.Handle]
	if !ok {
		return fmt.Errorf("failed to release unknown handle")
	}
	delete(fs.dirHandles, op.Handle)

	return nil
}

func (fs *filesystem) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	defer fs.synchronize(ctx)()

	nd, err := fs.nm.Open(ctx, fs.ptr(op.Inode))
	if err != nil {
		return err
	} else if !nd.Attrs.Mode.IsRegular() {
		return fuse.EINVAL
	}

	// Issue the next handle ID. It doesn't mean anything.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = struct{}{}
	op.Handle = handleID

	return nil
}

func (fs *filesystem) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	defer fs.synchronize(ctx)()

	nd, err := fs.nm.Open(ctx, fs.ptr(op.Inode))
	if err != nil {
		return err
	} else if !nd.Attrs.Mode.IsRegular() {
		return fuse.EINVAL
	}

	n := 0
	for n < len(op.Dst) {
		m, err := nd.ReadAt(op.Dst[n:], op.Offset+int64(n))
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		n += m
	}
	op.BytesRead = n

	return nil
}

func (fs *filesystem) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	return fs.writeFile(ctx, op, false)
}

func (fs *filesystem) writeFile(ctx context.Context, op *fuseops.WriteFileOp, archive bool) error {
	defer fs.synchronize(ctx)()

	nd, err := fs.nm.Open(ctx, fs.ptr(op.Inode))
	if err != nil {
		return err
	} else if !nd.Attrs.Mode.IsRegular() {
		return fuse.EINVAL
	}

	if archive {
		if err := checkForChanges(nd, op); err != nil {
			return err
		}
	}

	if _, err := nd.WriteAt(op.Data, op.Offset); err != nil {
		fs.nm.Forget(nd)
		return err
	}
	nd.Attrs.Mtime = now()

	return commit(ctx, fs.nm, nd)
}

func (fs *filesystem) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error {
	return nil
}

func (fs *filesystem) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) error {
	return nil
}

func (fs *filesystem) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) error {
	defer fs.synchronize(ctx)()

	_, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fmt.Errorf("failed to release unknown handle")
	}
	delete(fs.fileHandles, op.Handle)

	return nil
}

func (fs *filesystem) ReadSymlink(ctx context.Context, op *fuseops.ReadSymlinkOp) error {
	defer fs.synchronize(ctx)()

	nd, err := fs.nm.Open(ctx, fs.ptr(op.Inode))
	if err != nil {
		return err
	} else if nd.Attrs.Mode&os.ModeSymlink != os.ModeSymlink {
		return fuse.EINVAL
	}
	target, err := nd.ReadAll()
	if err != nil {
		return err
	}
	op.Target = string(target)

	return nil
}

func (fs *filesystem) mkNode(ctx context.Context, parentID fuseops.InodeID, name string, mode os.FileMode) (*node, *node, error) {
	childPtr, err := fs.nm.Create(ctx, mode)
	if err != nil {
		return nil, nil, err
	}
	childID := fs.inode(childPtr)

	parent, err := fs.nm.Open(ctx, fs.ptr(parentID))
	if err != nil {
		return nil, nil, err
	} else if !parent.Attrs.Mode.IsDir() {
		return nil, nil, fuse.ENOTDIR
	} else if _, ok := parent.Children[name]; ok {
		return nil, nil, fuse.EEXIST
	}
	child, err := fs.nm.Open(ctx, childPtr)
	if err != nil {
		return nil, nil, err
	}

	parent.Attrs.Mtime = now()
	parent.Attrs.Ctime = now()
	parent.Children[name] = childID

	return parent, child, nil
}

func (fs *filesystem) rmNode(ctx context.Context, parent *node, name string, archive bool) error {
	childID, ok := parent.Children[name]
	if !ok {
		return fuse.ENOENT
	}

	child, err := fs.nm.Open(ctx, fs.ptr(childID))
	if err != nil {
		return err
	} else if len(child.Children) > 0 {
		return fuse.ENOTEMPTY
	}
	fs.nm.Forget(child)
	child.Attrs.Nlink--
	if child.Attrs.Nlink == 0 {
		if archive && child.Attrs.Mode.IsRegular() {
			return fmt.Errorf("utahfs: refusing to delete archived file")
		} else if err := fs.nm.Unlink(ctx, fs.ptr(childID)); err != nil {
			return err
		}
	} else {
		child.Attrs.Ctime = now()
		if err := child.Persist(); err != nil {
			return err
		}
	}

	parent.Attrs.Mtime = now()
	parent.Attrs.Ctime = now()
	delete(parent.Children, name)

	return nil
}

func (fs *filesystem) synchronize(ctx context.Context) func() {
	fs.mu.Lock()
	if err := fs.nm.Start(ctx); err != nil {
		log.Println(err)
	}
	return func() {
		if r := recover(); r != nil {
			log.Println(r)
			log.Println(string(debug.Stack()))
			panic(r)
		}
		fs.nm.Rollback(ctx)
		fs.mu.Unlock()
	}
}

func (fs *filesystem) ptr(id fuseops.InodeID) uint64 {
	return uint64(id) + fs.rootPtr - 1
}

func (fs *filesystem) inode(ptr uint64) fuseops.InodeID {
	return fuseops.InodeID(ptr - fs.rootPtr + 1)
}

func (fs *filesystem) expiration() time.Time {
	return now().Add(time.Minute)
}
