package utahfs

import (
	"io"
	"log"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/billziss-gh/cgofuse/fuse"
)

func split(path string) []string {
	return strings.Split(path, "/")
}

func commit(nm *nodeManager, nds ...*node) int {
	for _, nd := range nds {
		if err := nd.Persist(); err != nil {
			log.Println(err)
			return -fuse.EIO
		}
	}
	if err := nm.Commit(); err != nil {
		log.Println(err)
		return -fuse.EIO
	}
	return 0
}

type filesystem struct {
	fuse.FileSystemBase

	nm      *nodeManager
	rootPtr uint64

	mu sync.Mutex
}

// NewFilesystem returns a FUSE binding that internally stores data in a
// block-based filesystem.
func NewFilesystem(store AppStorage, bfs *BlockFilesystem) (fuse.FileSystemInterface, error) {
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
	} else if state.RootPtr == nilPtr64 {
		rootPtr, err := nm.Create(0, fuse.S_IFDIR|00777, 0, 0)
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
	}, nil
}

func (fs *filesystem) Mknod(path string, mode uint32, dev uint64) int {
	defer fs.synchronize()()
	return fs.makeNode(path, mode, dev, nil)
}

func (fs *filesystem) Mkdir(path string, mode uint32) int {
	defer fs.synchronize()()
	return fs.makeNode(path, fuse.S_IFDIR|(mode&07777), 0, nil)
}

func (fs *filesystem) Unlink(path string) int {
	defer fs.synchronize()()
	return fs.removeNode(path, false, true)
}

func (fs *filesystem) Rmdir(path string) int {
	defer fs.synchronize()()
	return fs.removeNode(path, true, true)
}

func (fs *filesystem) Link(oldpath, newpath string) int {
	defer fs.synchronize()()

	_, oldnode, _, errc := fs.lookupNode(oldpath, nil)
	if errc != 0 {
		return errc
	} else if oldnode == nil {
		return -fuse.ENOENT
	}
	newprnt, newnode, newname, errc := fs.lookupNode(newpath, nil)
	if errc != 0 {
		return errc
	} else if newprnt == nil {
		return -fuse.ENOENT
	} else if newnode != nil {
		return -fuse.EEXIST
	}
	oldnode.Stat.Nlink++
	newprnt.Children[newname] = oldnode.Stat.Ino
	tmsp := fuse.Now()
	oldnode.Stat.Ctim = tmsp
	newprnt.Stat.Ctim = tmsp
	newprnt.Stat.Mtim = tmsp

	return commit(fs.nm, oldnode, newprnt)
}

func (fs *filesystem) Symlink(target, newpath string) int {
	defer fs.synchronize()()
	return fs.makeNode(newpath, fuse.S_IFLNK|00777, 0, []byte(target))
}

func (fs *filesystem) Readlink(path string) (int, string) {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc, ""
	} else if nd == nil {
		return -fuse.ENOENT, ""
	} else if nd.Stat.Mode&fuse.S_IFMT != fuse.S_IFLNK {
		return -fuse.EINVAL, ""
	}

	data, err := nd.ReadAll()
	if err != nil {
		log.Println(err)
		return -fuse.EIO, ""
	}
	return 0, string(data)
}

func (fs *filesystem) Rename(oldpath, newpath string) int {
	defer fs.synchronize()()

	oldprnt, oldnode, oldname, errc := fs.lookupNode(oldpath, nil)
	if errc != 0 {
		return errc
	} else if oldnode == nil {
		return -fuse.ENOENT
	}
	newprnt, newnode, newname, errc := fs.lookupNode(newpath, oldnode)
	if errc != 0 {
		return errc
	} else if newprnt == nil {
		return -fuse.ENOENT
	} else if newname == "" {
		// guard against directory loop creation
		return -fuse.EINVAL
	} else if oldprnt.Equals(newprnt) && oldname == newname {
		return 0
	}
	if newnode != nil {
		errc := fs.removeNode(newpath, fuse.S_IFDIR == oldnode.Stat.Mode&fuse.S_IFMT, false)
		if errc != 0 {
			return errc
		}
	}
	if oldprnt.Equals(newprnt) {
		delete(newprnt.Children, oldname)
		newprnt.Children[newname] = oldnode.Stat.Ino
	} else {
		delete(oldprnt.Children, oldname)
		newprnt.Children[newname] = oldnode.Stat.Ino
	}

	return commit(fs.nm, oldprnt, newprnt)
}

func (fs *filesystem) Chmod(path string, mode uint32) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	nd.Stat.Mode = (nd.Stat.Mode & fuse.S_IFMT) | mode&07777
	nd.Stat.Ctim = fuse.Now()

	return commit(fs.nm, nd)
}

func (fs *filesystem) Chown(path string, uid, gid uint32) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	if uid != ^uint32(0) {
		nd.Stat.Uid = uid
	}
	if gid != ^uint32(0) {
		nd.Stat.Gid = gid
	}
	nd.Stat.Ctim = fuse.Now()

	return commit(fs.nm, nd)
}

func (fs *filesystem) Utimens(path string, tmsp []fuse.Timespec) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	nd.Stat.Ctim = fuse.Now()
	if tmsp == nil {
		tmsp0 := nd.Stat.Ctim
		tmsa := [2]fuse.Timespec{tmsp0, tmsp0}
		tmsp = tmsa[:]
	}
	nd.Stat.Atim = tmsp[0]
	nd.Stat.Mtim = tmsp[1]

	return commit(fs.nm, nd)
}

func (fs *filesystem) Open(path string, flags int) (int, uint64) {
	defer fs.synchronize()()
	return fs.openNode(path, false)
}

func (fs *filesystem) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	defer fs.synchronize()()

	nd, errc := fs.getNode(path, fh)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	*stat = nd.Stat

	return 0
}

func (fs *filesystem) Truncate(path string, size int64, fh uint64) int {
	defer fs.synchronize()()

	nd, errc := fs.getNode(path, fh)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	} else if err := nd.Truncate(size); err != nil {
		log.Println(err)
		return -fuse.EIO
	}
	nd.Stat.Size = size
	tmsp := fuse.Now()
	nd.Stat.Ctim = tmsp
	nd.Stat.Mtim = tmsp

	return commit(fs.nm, nd)
}

func (fs *filesystem) Read(path string, buff []byte, offset int64, fh uint64) int {
	defer fs.synchronize()()

	nd, errc := fs.getNode(path, fh)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	n, err := nd.ReadAt(buff, offset)
	if err == io.EOF {
		return 0
	} else if err != nil {
		log.Println(path, offset, err)
		return -fuse.EIO
	}
	nd.Stat.Atim = fuse.Now()
	if errc := commit(fs.nm, nd); errc != 0 {
		return errc
	}

	return n
}

func (fs *filesystem) Write(path string, buff []byte, offset int64, fh uint64) int {
	defer fs.synchronize()()

	nd, errc := fs.getNode(path, fh)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	n, err := nd.WriteAt(buff, offset)
	if err != nil {
		log.Println(err)
		return -fuse.EIO
	}
	tmsp := fuse.Now()
	nd.Stat.Ctim = tmsp
	nd.Stat.Mtim = tmsp
	if errc := commit(fs.nm, nd); errc != 0 {
		return errc
	}

	return n
}

func (fs *filesystem) Release(path string, fh uint64) int {
	defer fs.synchronize()()
	return fs.closeNode(fh)
}

func (fs *filesystem) Opendir(path string) (int, uint64) {
	defer fs.synchronize()()
	return fs.openNode(path, true)
}

func (fs *filesystem) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, ptr uint64) int {
	defer fs.synchronize()()

	nd, err := fs.nm.Open(ptr)
	if err != nil {
		log.Println(err)
		return -fuse.EIO
	}

	fill(".", &nd.Stat, 0)
	fill("..", nil, 0)
	for name, ptr := range nd.Children {
		child, err := fs.nm.Open(ptr)
		if err != nil {
			log.Println(err)
			return -fuse.EIO
		} else if !fill(name, &child.Stat, 0) {
			break
		}
	}

	return 0
}

func (fs *filesystem) Releasedir(path string, fh uint64) int {
	defer fs.synchronize()()
	return fs.closeNode(fh)
}

func (fs *filesystem) Setxattr(path, name string, value []byte, flags int) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	} else if name == "com.apple.ResourceFork" {
		return -fuse.ENOTSUP
	}
	if fuse.XATTR_CREATE == flags {
		if _, ok := nd.XAttr[name]; ok {
			return -fuse.EEXIST
		}
	} else if fuse.XATTR_REPLACE == flags {
		if _, ok := nd.XAttr[name]; !ok {
			return -fuse.ENOATTR
		}
	}
	xatr := make([]byte, len(value))
	copy(xatr, value)
	if nd.XAttr == nil {
		nd.XAttr = make(map[string][]byte)
	}
	nd.XAttr[name] = xatr

	return commit(fs.nm, nd)
}

func (fs *filesystem) Getxattr(path, name string) (int, []byte) {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc, nil
	} else if nd == nil {
		return -fuse.ENOENT, nil
	} else if name == "com.apple.ResourceFork" {
		return -fuse.ENOTSUP, nil
	}
	xatr, ok := nd.XAttr[name]
	if !ok {
		return -fuse.ENOATTR, nil
	}

	return 0, xatr
}

func (fs *filesystem) Removexattr(path, name string) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	} else if name == "com.apple.ResourceFork" {
		return -fuse.ENOTSUP
	}
	if _, ok := nd.XAttr[name]; !ok {
		return -fuse.ENOATTR
	}
	delete(nd.XAttr, name)

	return commit(fs.nm, nd)
}

func (fs *filesystem) Listxattr(path string, fill func(name string) bool) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	for name := range nd.XAttr {
		if !fill(name) {
			return -fuse.ERANGE
		}
	}

	return 0
}

func (fs *filesystem) Chflags(path string, flags uint32) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	nd.Stat.Flags = flags
	nd.Stat.Ctim = fuse.Now()

	return commit(fs.nm, nd)
}

func (fs *filesystem) Setcrtime(path string, tmsp fuse.Timespec) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	nd.Stat.Birthtim = tmsp
	nd.Stat.Ctim = fuse.Now()

	return commit(fs.nm, nd)
}

func (fs *filesystem) Setchgtime(path string, tmsp fuse.Timespec) int {
	defer fs.synchronize()()

	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	}
	nd.Stat.Ctim = tmsp

	return commit(fs.nm, nd)
}

func (fs *filesystem) lookupNode(path string, ancestor *node) (prnt, nd *node, name string, errc int) {
	root, err := fs.nm.Open(fs.rootPtr)
	if err != nil {
		log.Println(err)
		return nil, nil, "", -fuse.EIO
	}
	prnt, nd, name = root, root, ""

	for _, c := range split(path) {
		if len(c) >= 255 {
			panic(fuse.Error(-fuse.ENAMETOOLONG))
		} else if c != "" {
			var child *node

			ptr, ok := nd.Children[c]
			if ok {
				var err error
				child, err = fs.nm.Open(ptr)
				if err != nil {
					log.Println(err)
					return nil, nil, "", -fuse.EIO
				}
			}

			prnt, nd, name = nd, child, c

			if ancestor != nil && nd.Equals(ancestor) {
				name = "" // special case loop condition
				return
			}
		}
	}

	return
}

func (fs *filesystem) makeNode(path string, mode uint32, dev uint64, data []byte) int {
	prnt, nd, name, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if prnt == nil {
		return -fuse.ENOENT
	} else if nd != nil {
		return -fuse.EEXIST
	}
	uid, gid, _ := fuse.Getcontext()
	ptr, err := fs.nm.Create(dev, mode, uid, gid)
	if err != nil {
		log.Println(err)
		return -fuse.EIO
	}
	nd, err = fs.nm.Open(ptr)
	if err != nil {
		log.Println(err)
		return -fuse.EIO
	}

	if len(data) > 0 {
		if _, err := nd.WriteAt(data, 0); err != nil {
			log.Println(err)
			return -fuse.EIO
		}
		nd.Stat.Size = int64(len(data))
	}
	prnt.Children[name] = ptr
	prnt.Stat.Ctim = nd.Stat.Ctim
	prnt.Stat.Mtim = nd.Stat.Ctim

	return commit(fs.nm, prnt, nd)
}

func (fs *filesystem) removeNode(path string, dir, end bool) int {
	prnt, nd, name, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc
	} else if nd == nil {
		return -fuse.ENOENT
	} else if !dir && nd.Stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		return -fuse.EISDIR
	} else if dir && nd.Stat.Mode&fuse.S_IFMT != fuse.S_IFDIR {
		return -fuse.ENOTDIR
	} else if len(nd.Children) > 0 {
		return -fuse.ENOTEMPTY
	}
	tmsp := fuse.Now()

	nd.Stat.Nlink--
	if nd.Stat.Nlink == 0 {
		if err := fs.nm.Unlink(nd.Stat.Ino); err != nil {
			log.Println(err)
			return -fuse.EIO
		}
	} else {
		nd.Stat.Ctim = tmsp
		if err := nd.Persist(); err != nil {
			log.Println(err)
			return -fuse.EIO
		}
	}

	delete(prnt.Children, name)
	prnt.Stat.Ctim = tmsp
	prnt.Stat.Mtim = tmsp

	if end {
		return commit(fs.nm, prnt)
	}
	return 0
}

func (fs *filesystem) openNode(path string, dir bool) (int, uint64) {
	_, nd, _, errc := fs.lookupNode(path, nil)
	if errc != 0 {
		return errc, ^uint64(0)
	} else if nd == nil {
		return -fuse.ENOENT, ^uint64(0)
	} else if !dir && nd.Stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		return -fuse.EISDIR, ^uint64(0)
	} else if dir && nd.Stat.Mode&fuse.S_IFMT != fuse.S_IFDIR {
		return -fuse.ENOTDIR, ^uint64(0)
	}
	return 0, uint64(nd.Stat.Ino)
}

func (fs *filesystem) closeNode(fh uint64) int {
	return 0
}

func (fs *filesystem) getNode(path string, fh uint64) (*node, int) {
	if fh == ^uint64(0) {
		_, nd, _, errc := fs.lookupNode(path, nil)
		return nd, errc
	} else {
		nd, err := fs.nm.Open(fh)
		if err != nil {
			log.Println(err)
			return nil, -fuse.EIO
		}
		return nd, 0
	}
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
