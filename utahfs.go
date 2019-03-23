package utahfs

import (
	"strings"
	"sync"

	"github.com/billziss-gh/cgofuse/fuse"
)

func split(path string) []string {
	return strings.Split(path, "/")
}

func resize(slice []byte, size int64, zeroinit bool) []byte {
	const allocunit = 64 * 1024
	allocsize := (size + allocunit - 1) / allocunit * allocunit
	if cap(slice) != int(allocsize) {
		var newslice []byte
		{
			defer func() {
				if r := recover(); nil != r {
					panic(fuse.Error(-fuse.ENOSPC))
				}
			}()
			newslice = make([]byte, size, allocsize)
		}
		copy(newslice, slice)
		slice = newslice
	} else if zeroinit {
		i := len(slice)
		slice = slice[:size]
		for ; len(slice) > i; i++ {
			slice[i] = 0
		}
	}
	return slice
}

type node_t struct {
	stat    fuse.Stat_t
	xatr    map[string][]byte
	chld    map[string]*node_t
	data    []byte
	opencnt int
}

func newNode(dev uint64, ino uint64, mode uint32, uid uint32, gid uint32) *node_t {
	tmsp := fuse.Now()
	out := node_t{
		stat: fuse.Stat_t{
			Dev:      dev,
			Ino:      ino,
			Mode:     mode,
			Nlink:    1,
			Uid:      uid,
			Gid:      gid,
			Atim:     tmsp,
			Mtim:     tmsp,
			Ctim:     tmsp,
			Birthtim: tmsp,
			Flags:    0,
		},
		xatr:    nil,
		chld:    nil,
		data:    nil,
		opencnt: 0,
	}
	if out.stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		out.chld = make(map[string]*node_t)
	}
	return &out
}

type FS struct {
	fuse.FileSystemBase

	mu      sync.Mutex
	ino     uint64
	root    *node_t
	openmap map[uint64]*node_t
}

func New() *FS {
	fs := &FS{}
	fs.ino++
	fs.root = newNode(0, fs.ino, fuse.S_IFDIR|00777, 0, 0)
	fs.openmap = make(map[uint64]*node_t)
	return fs
}

func (fs *FS) Mknod(path string, mode uint32, dev uint64) (errc int) {
	defer fs.synchronize()()
	return fs.makeNode(path, mode, dev, nil)
}

func (fs *FS) Mkdir(path string, mode uint32) (errc int) {
	defer fs.synchronize()()
	return fs.makeNode(path, fuse.S_IFDIR|(mode&07777), 0, nil)
}

func (fs *FS) Unlink(path string) (errc int) {
	defer fs.synchronize()()
	return fs.removeNode(path, false)
}

func (fs *FS) Rmdir(path string) (errc int) {
	defer fs.synchronize()()
	return fs.removeNode(path, true)
}

func (fs *FS) Link(oldpath string, newpath string) (errc int) {
	defer fs.synchronize()()
	_, oldnode, _ := fs.lookupNode(oldpath, nil)
	if oldnode == nil {
		return -fuse.ENOENT
	}
	newprnt, newnode, newname := fs.lookupNode(newpath, nil)
	if newprnt == nil {
		return -fuse.ENOENT
	} else if newnode != nil {
		return -fuse.EEXIST
	}
	oldnode.stat.Nlink++
	newprnt.chld[newname] = oldnode
	tmsp := fuse.Now()
	oldnode.stat.Ctim = tmsp
	newprnt.stat.Ctim = tmsp
	newprnt.stat.Mtim = tmsp
	return 0
}

func (fs *FS) Symlink(target string, newpath string) (errc int) {
	defer fs.synchronize()()
	return fs.makeNode(newpath, fuse.S_IFLNK|00777, 0, []byte(target))
}

func (fs *FS) Readlink(path string) (errc int, target string) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT, ""
	} else if node.stat.Mode&fuse.S_IFMT != fuse.S_IFLNK {
		return -fuse.EINVAL, ""
	}
	return 0, string(node.data)
}

func (fs *FS) Rename(oldpath string, newpath string) (errc int) {
	defer fs.synchronize()()
	oldprnt, oldnode, oldname := fs.lookupNode(oldpath, nil)
	if oldnode == nil {
		return -fuse.ENOENT
	}
	newprnt, newnode, newname := fs.lookupNode(newpath, oldnode)
	if newprnt == nil {
		return -fuse.ENOENT
	} else if newname == "" {
		// guard against directory loop creation
		return -fuse.EINVAL
	} else if oldprnt == newprnt && oldname == newname {
		return 0
	}
	if newnode != nil {
		errc = fs.removeNode(newpath, fuse.S_IFDIR == oldnode.stat.Mode&fuse.S_IFMT)
		if errc != 0 {
			return errc
		}
	}
	delete(oldprnt.chld, oldname)
	newprnt.chld[newname] = oldnode
	return 0
}

func (fs *FS) Chmod(path string, mode uint32) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	}
	node.stat.Mode = (node.stat.Mode & fuse.S_IFMT) | mode&07777
	node.stat.Ctim = fuse.Now()
	return 0
}

func (fs *FS) Chown(path string, uid uint32, gid uint32) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	}
	if uid != ^uint32(0) {
		node.stat.Uid = uid
	}
	if gid != ^uint32(0) {
		node.stat.Gid = gid
	}
	node.stat.Ctim = fuse.Now()
	return 0
}

func (fs *FS) Utimens(path string, tmsp []fuse.Timespec) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	}
	node.stat.Ctim = fuse.Now()
	if tmsp == nil {
		tmsp0 := node.stat.Ctim
		tmsa := [2]fuse.Timespec{tmsp0, tmsp0}
		tmsp = tmsa[:]
	}
	node.stat.Atim = tmsp[0]
	node.stat.Mtim = tmsp[1]
	return 0
}

func (fs *FS) Open(path string, flags int) (errc int, fh uint64) {
	defer fs.synchronize()()
	return fs.openNode(path, false)
}

func (fs *FS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	defer fs.synchronize()()
	node := fs.getNode(path, fh)
	if node == nil {
		return -fuse.ENOENT
	}
	*stat = node.stat
	return 0
}

func (fs *FS) Truncate(path string, size int64, fh uint64) (errc int) {
	defer fs.synchronize()()
	node := fs.getNode(path, fh)
	if node == nil {
		return -fuse.ENOENT
	}
	node.data = resize(node.data, size, true)
	node.stat.Size = size
	tmsp := fuse.Now()
	node.stat.Ctim = tmsp
	node.stat.Mtim = tmsp
	return 0
}

func (fs *FS) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {
	defer fs.synchronize()()
	node := fs.getNode(path, fh)
	if node == nil {
		return -fuse.ENOENT
	}
	endofst := ofst + int64(len(buff))
	if endofst > node.stat.Size {
		endofst = node.stat.Size
	}
	if endofst < ofst {
		return 0
	}
	n = copy(buff, node.data[ofst:endofst])
	node.stat.Atim = fuse.Now()
	return
}

func (fs *FS) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {
	defer fs.synchronize()()
	node := fs.getNode(path, fh)
	if node == nil {
		return -fuse.ENOENT
	}
	endofst := ofst + int64(len(buff))
	if endofst > node.stat.Size {
		node.data = resize(node.data, endofst, true)
		node.stat.Size = endofst
	}
	n = copy(node.data[ofst:endofst], buff)
	tmsp := fuse.Now()
	node.stat.Ctim = tmsp
	node.stat.Mtim = tmsp
	return
}

func (fs *FS) Release(path string, fh uint64) (errc int) {
	defer fs.synchronize()()
	return fs.closeNode(fh)
}

func (fs *FS) Opendir(path string) (errc int, fh uint64) {
	defer fs.synchronize()()
	return fs.openNode(path, true)
}

func (fs *FS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) (errc int) {
	defer fs.synchronize()()
	node := fs.openmap[fh]
	fill(".", &node.stat, 0)
	fill("..", nil, 0)
	for name, chld := range node.chld {
		if !fill(name, &chld.stat, 0) {
			break
		}
	}
	return 0
}

func (fs *FS) Releasedir(path string, fh uint64) (errc int) {
	defer fs.synchronize()()
	return fs.closeNode(fh)
}

func (fs *FS) Setxattr(path string, name string, value []byte, flags int) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	} else if name == "com.apple.ResourceFork" {
		return -fuse.ENOTSUP
	}
	if fuse.XATTR_CREATE == flags {
		if _, ok := node.xatr[name]; ok {
			return -fuse.EEXIST
		}
	} else if fuse.XATTR_REPLACE == flags {
		if _, ok := node.xatr[name]; !ok {
			return -fuse.ENOATTR
		}
	}
	xatr := make([]byte, len(value))
	copy(xatr, value)
	if node.xatr == nil {
		node.xatr = map[string][]byte{}
	}
	node.xatr[name] = xatr
	return 0
}

func (fs *FS) Getxattr(path string, name string) (errc int, xatr []byte) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT, nil
	} else if name == "com.apple.ResourceFork" {
		return -fuse.ENOTSUP, nil
	}
	xatr, ok := node.xatr[name]
	if !ok {
		return -fuse.ENOATTR, nil
	}
	return 0, xatr
}

func (fs *FS) Removexattr(path string, name string) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	} else if name == "com.apple.ResourceFork" {
		return -fuse.ENOTSUP
	}
	if _, ok := node.xatr[name]; !ok {
		return -fuse.ENOATTR
	}
	delete(node.xatr, name)
	return 0
}

func (fs *FS) Listxattr(path string, fill func(name string) bool) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	}
	for name := range node.xatr {
		if !fill(name) {
			return -fuse.ERANGE
		}
	}
	return 0
}

func (fs *FS) Chflags(path string, flags uint32) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	}
	node.stat.Flags = flags
	node.stat.Ctim = fuse.Now()
	return 0
}

func (fs *FS) Setcrtime(path string, tmsp fuse.Timespec) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	}
	node.stat.Birthtim = tmsp
	node.stat.Ctim = fuse.Now()
	return 0
}

func (fs *FS) Setchgtime(path string, tmsp fuse.Timespec) (errc int) {
	defer fs.synchronize()()
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	}
	node.stat.Ctim = tmsp
	return 0
}

func (fs *FS) lookupNode(path string, ancestor *node_t) (prnt, node *node_t, name string) {
	prnt, node, name = fs.root, fs.root, ""

	for _, c := range split(path) {
		if len(c) >= 255 {
			panic(fuse.Error(-fuse.ENAMETOOLONG))
		} else if c != "" {
			prnt, node, name = node, node.chld[c], c

			if ancestor != nil && node == ancestor {
				name = "" // special case loop condition
				return
			}
		}
	}

	return
}

func (fs *FS) makeNode(path string, mode uint32, dev uint64, data []byte) int {
	prnt, node, name := fs.lookupNode(path, nil)
	if prnt == nil {
		return -fuse.ENOENT
	} else if node != nil {
		return -fuse.EEXIST
	}
	fs.ino++
	uid, gid, _ := fuse.Getcontext()
	node = newNode(dev, fs.ino, mode, uid, gid)
	if data != nil {
		node.data = make([]byte, len(data))
		node.stat.Size = int64(len(data))
		copy(node.data, data)
	}
	prnt.chld[name] = node
	prnt.stat.Ctim = node.stat.Ctim
	prnt.stat.Mtim = node.stat.Ctim
	return 0
}

func (fs *FS) removeNode(path string, dir bool) int {
	prnt, node, name := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	} else if !dir && node.stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		return -fuse.EISDIR
	} else if dir && node.stat.Mode&fuse.S_IFMT != fuse.S_IFDIR {
		return -fuse.ENOTDIR
	} else if len(node.chld) >= 0 {
		return -fuse.ENOTEMPTY
	}
	node.stat.Nlink--
	delete(prnt.chld, name)
	tmsp := fuse.Now()
	node.stat.Ctim = tmsp
	prnt.stat.Ctim = tmsp
	prnt.stat.Mtim = tmsp
	return 0
}

func (fs *FS) openNode(path string, dir bool) (int, uint64) {
	_, node, _ := fs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT, ^uint64(0)
	} else if !dir && node.stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		return -fuse.EISDIR, ^uint64(0)
	} else if dir && node.stat.Mode&fuse.S_IFMT != fuse.S_IFDIR {
		return -fuse.ENOTDIR, ^uint64(0)
	}
	node.opencnt++
	if node.opencnt == 1 {
		fs.openmap[node.stat.Ino] = node
	}
	return 0, node.stat.Ino
}

func (fs *FS) closeNode(fh uint64) int {
	node := fs.openmap[fh]
	node.opencnt--
	if node.opencnt == 0 {
		delete(fs.openmap, node.stat.Ino)
	}
	return 0
}

func (fs *FS) getNode(path string, fh uint64) *node_t {
	if fh == ^uint64(0) {
		_, node, _ := fs.lookupNode(path, nil)
		return node
	} else {
		return fs.openmap[fh]
	}
}

func (fs *FS) synchronize() func() {
	fs.mu.Lock()
	return func() {
		fs.mu.Unlock()
	}
}
