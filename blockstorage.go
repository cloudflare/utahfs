package utahfs

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	nilPtr = ^uint32(0)

	numPtrs  int64 = 12
	dataSize int64 = 32 * 1024

	blockSize int64 = 4 + 4*numPtrs + 3 + dataSize
)

var (
	errEndOfBlock    = fmt.Errorf("reached end of block")
	errInvalidOffset = fmt.Errorf("invalid offset to read from block")
)

// blockStorage implements large files as skiplists over fixed-size blocks
// stored in an object storage service.
type blockStorage struct {
	store       ObjectStorage
	trash, next uint32
}

// allocate returns the pointer of a block which is free for use by the caller.
func (bs *blockStorage) allocate() (uint32, error) {
	if bs.trash == nilPtr {
		next := bs.next
		bs.next += 1
		return next, nil
	}

	data, err := bs.store.Get(fmt.Sprintf("%x", bs.trash))
	if err != nil {
		return nilPtr, fmt.Errorf("failed to load block %x: %v", ptr, err)
	}
	b, err := parseBlock(data)
	if err != nil {
		return nilPtr, fmt.Errorf("failed to parse block %x: %v", ptr, err)
	}

	trash := bs.trash
	bs.trash = b.ptrs[0]
	return trash, nil
}

func (bs *blockStorage) Create() (uint32, *blockFile, error) {
	ptr, err := bs.allocate()
}

// func (bs *blockStorage) Open(ptr uint64) (*blockFile, error)

// blockFile implements read-write functionality for a variable-size file over
// a skiplist of fixed-size blocks.
type blockFile struct {
	parent *blockStorage

	// start points to the first block of the file.
	start uint32
	// size is the total size of the file, in bytes.
	size int64

	// ptr is the pointer for the current block of the file.
	ptr uint32
	// pos is our current position in the file, in bytes.
	pos int64
	// curr is the parsed version of the current block.
	curr *block
}

// persist saves any changes to the current node to the data storage backend.
func (bf *blockFile) persist() error {
	return bf.parent.Set(fmt.Sprintf("%x", bf.ptr), bf.curr.Marshal())
}

// load pulls the block at `ptr` into memory. `pos` is our new position in the
// file.
func (bf *blockFile) load(ptr uint32, pos int64) error {
	data, err := bf.parent.store.Get(fmt.Sprintf("%x", ptr))
	if err != nil {
		return fmt.Errorf("failed to load block %x: %v", ptr, err)
	}
	curr, err := parseBlock(data)
	if err != nil {
		return fmt.Errorf("failed to parse block %x: %v", ptr, err)
	}

	b.ptr = ptr
	b.pos = pos
	b.curr = curr

	return nil
}

func (bf *blockFile) Read(p []byte) (int, error) {
	n, err := bf.read(p)
	bf.pos += n
	return n, err
}

func (bf *blockFile) read(p []byte) (int, error) {
	n, err := bf.curr.ReadAt(p, bf.pos)
	if err == errEndOfBlock {
		if bf.curr.ptrs[0] == nilPtr {
			return 0, io.EOF
		} else if err := bf.load(bf.curr.ptrs[0], bf.pos); err != nil {
			return 0, err
		}
		return bf.curr.ReadAt(p, bf.pos)
	}

	return n, err
}

func (bf *blockFile) Write(p []byte) (int, error) {
	n, err := bf.write(p)
	bf.pos += n
	return n, err
}

func (bf *blockFile) write(p []byte) (int, error) {
	n := 0

	for n < len(p) {
		m, err := bf.curr.WriteAt(p, bf.pos)
		n += m
		bf.pos += m

		if err == nil {
			continue
		} else if err != errEndOfBlock {
			return n, err
		} // else err == errEndOfBlock

		// Check if the next block already exists and just write over it if so.
		if bf.curr.ptrs[0] != nil {
			if err := bf.persist(); err != nil {
				return n, err
			} else if err := bf.load(bf.curr.ptrs[0], bf.pos); err != nil {
				return n, err
			}
			continue
		}

		// There is no next block. We have to create it.
		ptr, err := bf.parent.allocate()
		if err != nil {
			return n, err
		}

		idx := bf.curr + 1
		ptrs := make([]uint32, numPtrs)
		copy(ptrs, bf.curr.ptrs)

		bf.curr.ptrs[0] = ptr
		for i := 1; i < len(bf.curr.ptrs); i++ {
			bf.curr.ptrs[i] = nilPtr
		}
		if err := bf.persist(); err != nil {
			return n, err
		}

		bf.curr = &block{
			idx:  idx,
			ptrs: ptrs,
		}
		// Go through and update nodes with our pointer.
	}

	return n, nil
}

func (bf *blockFile) Seek(offset int64, whence int) (int64, error) {
	// Calculate offset relative to the beginning of the file.
	if whence == io.SeekStart {
		// offset = offset
	} else if whence == io.SeekCurrent {
		offset = bf.pos + offset
	} else if whence == io.SeekEnd {
		offset = bf.size - offset
	} else {
		return -1, fmt.Errorf("unexpected value for whence")
	}

	if offset < 0 {
		return -1, fmt.Errorf("cannot seek past beginning of file")
	} else if offset > bf.size {
		return -1, fmt.Errorf("cannot seek past end of file")
	}

	// Follow the skiplist.
	if offset < bf.pos {
		if err := bf.load(bf.start, 0); err != nil {
			return -1, err
		}
	}

	for bf.pos != offset {
		pos := bf.pos / dataSize * dataSize

		// See if we have what we need in-memory.
		if d := offset - pos; d < dataSize {
			bf.pos += d
			bf.curr.Seek(int(d))
			return offset, nil
		}

		// We need to load another block. Choose the next pointer to follow.
		if bf.curr.ptrs[0] == nilPtr {
			return -1, fmt.Errorf("unexpectedly reached end of skiplist")
		}
		stepped := false

		for i := len(bf.curr.ptrs) - 1; i >= 0; i-- {
			if bf.curr.ptrs[i] == nilPtr {
				continue
			}
			jump := (1<<uint(i))*dataSize + pos
			if jump > offset {
				continue
			}

			// This pointer will get us as far as possible without going over.
			if err := bf.load(bf.curr.ptrs[i], jump); err != nil {
				return -1, err
			}
			stepped = true
			break
		}

		if !stepped { // This error should only ever occur if the skiplist is corrupted.
			return -1, fmt.Errorf("failed to find a suitable pointer in skiplist")
		}
	}

	return bf.pos, nil
}

type block struct {
	idx  uint32   // idx is the index of this block in the skiplist.
	ptrs []uint32 // ptrs contains the skiplist pointers from the current block.
	data []byte   // data is the block's application data.
}

func parseBlock(raw []byte) (*block, error) {
	if len(raw) != blockSize {
		return nil, fmt.Errorf("unexpected size: %v != %v", len(raw), blockSize)
	}

	// Read index.
	idx := readInt(raw[:4])
	raw = raw[4:]

	// Read pointers.
	b.ptrs = make([]uint32, numPtrs)
	for i := 0; i < len(b.ptrs); i++ {
		b.ptrs[i] = uint32(readInt(raw[:4]))
		raw = raw[4:]
	}

	// Read length of application data.
	size := readInt(raw[:3])
	raw = raw[3:]
	if len(raw) < size {
		return nil, fmt.Errorf("application data has unexpected size")
	}

	return &block{idx, ptrs, raw[:size]}
}

func (b *block) ReadAt(p []byte, off int64) (int, error) {
	off = off - int64(b.idx)*dataSize
	if off == dataSize {
		return 0, errEndOfBlock
	} else if off < 0 || off > dataSize {
		return 0, errInvalidOffset
	} else if off > len(b.data) {
		return 0, io.EOF
	}

	n := copy(p, b.data[off:])
	return n, nil
}

func (b *block) WriteAt(p []byte, off int64) (int, error) {
	off = off - int64(b.idx)*dataSize
	if off == dataSize {
		return 0, errEndOfBlock
	} else if off < 0 || off > dataSize {
		return 0, errInvalidOffset
	}

	// Expand b.data if necessary.
	end := off + int64(len(p))
	if end > dataSize {
		end = dataSize
	}
	if end > int64(len(b.data)) {
		temp := make([]byte, end)
		copy(temp, b.data)
		b.data = temp
	}

	n := copy(b.data[off:], p)
	return n, nil
}

func (b *block) Marshal() []byte {
	out := make([]byte, blockSize)
	rest := out[0:]

	// Write index.
	writeInt(b.idx, rest[:3])
	rest = rest[3:]

	// Write pointers.
	for i := 0; i < len(b.ptrs); i++ {
		writeInt(b.ptrs[i], rest[:4])
		rest = rest[4:]
	}

	// Write length of application data.
	writeInt(len(b.data), rest[:3])
	rest = rest[3:]

	// Write application data.
	copy(rest, b.data)

	return out
}

func readInt(in []byte) int {
	out := 0
	for i := len(in) - 1; i >= 0; i-- {
		out = out<<8 + int(in[i])
	}
	return out
}

func writeInt(in int, out []byte) {
	for i := 0; i < len(out); i++ {
		out[i] = byte(in)
		in = in >> 8
	}
}
