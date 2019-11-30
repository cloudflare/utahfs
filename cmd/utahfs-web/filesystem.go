package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type FileSystem struct {
	fs fuseutil.FileSystem
}

func (fs *FileSystem) Open(name string) (http.File, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if name == "/" {
		op := &fuseops.GetInodeAttributesOp{Inode: fuseops.RootInodeID}
		if err := fs.fs.GetInodeAttributes(ctx, op); err != nil {
			return nil, err
		}
		return &File{
			fs: fs.fs,

			inode: fuseops.RootInodeID,
			fi:    newFileInfo("", op.Attributes),
		}, nil
	}

	var (
		inode = fuseops.InodeID(fuseops.RootInodeID)
		fi    *FileInfo
	)
	for _, part := range strings.Split(name, "/")[1:] {
		op := &fuseops.LookUpInodeOp{Parent: inode, Name: part}
		if err := fs.fs.LookUpInode(ctx, op); err != nil {
			return nil, err
		}
		inode = op.Entry.Child
		fi = newFileInfo(part, op.Entry.Attributes)
	}

	return &File{
		fs: fs.fs,

		inode: inode,
		fi:    fi,
	}, nil
}

type File struct {
	fs fuseutil.FileSystem

	inode fuseops.InodeID
	fi    *FileInfo
	pos   int64
}

func (f *File) Close() error { return nil }

func (f *File) Read(p []byte) (int, error) {
	if f.pos == f.fi.size {
		return 0, io.EOF
	}
	op := &fuseops.ReadFileOp{Inode: f.inode, Offset: f.pos, Dst: p}
	if err := f.fs.ReadFile(context.Background(), op); err != nil {
		return 0, err
	}
	f.pos += int64(op.BytesRead)
	return op.BytesRead, nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart {
		// Offset is already in correct form.
	} else if whence == io.SeekCurrent {
		offset += f.pos
	} else if whence == io.SeekEnd {
		offset = f.fi.size - offset
	} else {
		return 0, fmt.Errorf("unexpected value for whence")
	}

	if offset > f.fi.size {
		return 0, fmt.Errorf("cannot seek past end of file")
	}
	f.pos = offset
	return offset, nil
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	if !f.fi.IsDir() {
		return nil, fmt.Errorf("file is not a directory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	open := &fuseops.OpenDirOp{Inode: f.inode}
	if err := f.fs.OpenDir(ctx, open); err != nil {
		return nil, err
	}
	defer func() {
		release := &fuseops.ReleaseDirHandleOp{Handle: open.Handle}
		f.fs.ReleaseDirHandle(ctx, release)
	}()

	entries := make([]os.FileInfo, 0)
	for {
		// Read the next chunk of entries from the directory.
		dst := make([]byte, 2048)
		op := &fuseops.ReadDirOp{
			Inode:  f.inode,
			Handle: open.Handle,

			Offset: fuseops.DirOffset(len(entries)),
			Dst:    dst,
		}
		if err := f.fs.ReadDir(ctx, op); err != nil {
			return nil, err
		} else if op.BytesRead == 0 {
			break
		}
		dst = dst[:op.BytesRead]

		// Parse the names returned.
		for len(dst) > 0 {
			var (
				de  fuseutil.Dirent
				err error
			)
			dst, de, err = parseDirent(dst)
			if err != nil {
				return nil, err
			}

			// Look up the full info for the entry and convert it to a FileInfo.
			op := &fuseops.LookUpInodeOp{Parent: f.inode, Name: de.Name}
			if err := f.fs.LookUpInode(ctx, op); err != nil {
				return nil, err
			}
			entries = append(entries, newFileInfo(de.Name, op.Entry.Attributes))
		}
	}

	return entries, nil
}

func (f *File) Stat() (os.FileInfo, error) { return f.fi, nil }

type FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

func newFileInfo(name string, attrs fuseops.InodeAttributes) *FileInfo {
	return &FileInfo{
		name:    name,
		size:    int64(attrs.Size),
		mode:    attrs.Mode,
		modTime: attrs.Mtime,
	}
}

func (fi *FileInfo) Name() string       { return fi.name }
func (fi *FileInfo) Size() int64        { return fi.size }
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *FileInfo) IsDir() bool        { return fi.Mode().IsDir() }
func (fi *FileInfo) Sys() interface{}   { return nil }

func parseDirent(buf []byte) ([]byte, fuseutil.Dirent, error) {
	type fuse_dirent struct {
		ino     uint64
		off     uint64
		namelen uint32
		type_   uint32
		name    [0]byte
	}

	const direntAlignment = 8
	const direntSize = 8 + 8 + 4 + 4

	if len(buf) < direntSize {
		return nil, fuseutil.Dirent{}, fmt.Errorf("buffer is too short")
	}
	de := fuse_dirent{}

	n := copy((*[direntSize]byte)(unsafe.Pointer(&de))[:], buf)
	buf = buf[n:]

	if len(buf) < int(de.namelen) {
		return nil, fuseutil.Dirent{}, fmt.Errorf("buffer is too short")
	}
	name := string(buf[:de.namelen])
	buf = buf[de.namelen:]

	var padLen int
	if len(name)%direntAlignment != 0 {
		padLen = direntAlignment - (len(name) % direntAlignment)
	}
	if len(buf) < padLen {
		return nil, fuseutil.Dirent{}, fmt.Errorf("buffer is too short")
	}
	buf = buf[padLen:]

	return buf, fuseutil.Dirent{
		Offset: fuseops.DirOffset(de.off),

		Inode: fuseops.InodeID(de.ino),
		Name:  name,

		Type: fuseutil.DirentType(de.type_),
	}, nil
}
