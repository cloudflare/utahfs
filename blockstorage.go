package utahfs

import (
	"encoding/binary"
	"fmt"
	"io"
)

// NOTE: For reliability, we'd need trash and next ptrs to rollback if there's
// an error.

const (
	nilPtr = ^uint32(0)

	numPtrs  int64 = 12
	dataSize int64 = 32 * 1024

	blockSize int64 = 4*numPtrs + 3 + dataSize
)

var (
	errEndOfBlock    = fmt.Errorf("reached end of block")
	errInvalidOffset = fmt.Errorf("invalid offset to read from block")
)

// blockStorage implements large files as skiplists over fixed-size blocks
// stored in an object storage service.
type blockStorage struct {
	store ObjectStorage
	// trash points to the first block of the trash list -- a linked list of
	// blocks which have been discarded and are free for re-allocation.
	trash uint32
	// next is the next unallocated pointer. A block with this pointer is
	// created only if the trash list is empty.
	next uint32
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

// Create creates a new file. It returns the pointer to the file and an open
// copy.
func (bs *blockStorage) Create() (uint32, *blockFile, error) {
	ptr, err := bs.allocate()
	if err != nil {
		return
	}

	ptrs := make([]uint32, numPtrs)
	for i := 0; i < len(ptrs); i++ {
		ptrs[i] = nilPtr
	}

	bf := &blockFile{
		parent: bs,

		start: ptr,
		size:  0,

		pos:  0,
		idx:  0,
		ptr:  ptr,
		curr: &block{ptrs: ptrs},
	}
	if err := bf.persist(); err != nil {
		return nilPtr, nil, err
	}

	return ptr, bf, nil
}

func (bs *blockStorage) Open(ptr uint64) (*blockFile, error) {
	bf := &blockFile{
		parent: bs,

		start: ptr,
		size:  0,
	}
	if err := bf.load(ptr, 0); err != nil {
		return nil, err
	}

	return bf, nil
}

// blockFile implements read-write functionality for a variable-size file over
// a skiplist of fixed-size blocks.
type blockFile struct {
	parent *blockStorage

	// start points to the first block of the file.
	start uint32
	// size is the total size of the file, in bytes.
	size int64

	// pos is our current position in the file, in bytes.
	pos int64
	// idx is the index of this block in the skiplist.
	idx int64
	// ptr is the pointer for the current block of the file.
	ptr uint32
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

	b.pos = pos
	b.idx = pos / dataSize
	b.ptr = ptr
	b.curr = curr

	return nil
}

func (bf *blockFile) Read(p []byte) (int, error) {
	n, err := bf.read(p)
	bf.pos += n
	return n, err
}

func (bf *blockFile) read(p []byte) (int, error) {
	n, err := bf.readAt(p, bf.pos)
	if err == errEndOfBlock {
		if bf.curr.ptrs[0] == nilPtr {
			return 0, io.EOF
		} else if err := bf.load(bf.curr.ptrs[0], bf.pos); err != nil {
			return 0, err
		}
		return bf.readAt(p, bf.pos)
	}

	return n, err
}

func (bf *blockFile) readAt(p []byte, offset int64) (int, error) {
	offset = offset - bf.idx*dataSize
	if offset == dataSize {
		return 0, errEndOfBlock
	} else if offset < 0 || offset > dataSize {
		return 0, errInvalidOffset
	} else if offset > len(bf.curr.data) {
		return 0, io.EOF
	}

	n := copy(p, bf.curr.data[offset:])
	return n, nil
}

func (bf *blockFile) Write(p []byte) (int, error) {
	n := 0

	for n < len(p) {
		m, err := bf.write(p)

		n += m
		bf.pos += m
		if bf.pos > bf.size {
			bf.size = bf.pos
		}

		if err != nil {
			return n, err
		}
	}

	return n, bf.persist()
}

func (bf *blockFile) write(p []byte) (int, error) {
	n, err := bf.writeAt(p, bf.pos)
	if err == nil {
		return n, nil
	} else if err != errEndOfBlock {
		return n, err
	} // else err == errEndOfBlock

	// Check if the next block already exists and just write over it if so.
	if bf.curr.ptrs[0] != nilPtr {
		if err := bf.persist(); err != nil {
			return 0, err
		} else if err := bf.load(bf.curr.ptrs[0], bf.pos); err != nil {
			return 0, err
		}
		return bf.writeAt(p, bf.pos)
	}

	// There is no next block. We have to create it. First thing is to change
	// the format of the current block from a tail to an intermediate.
	ptr, err := bf.parent.allocate()
	if err != nil {
		return 0, err
	}
	ptrs := bf.curr.Upgrade(bf.idx, bf.ptr, ptr)
	if err := bf.persist(); err != nil {
		return 0, err
	}

	// Load all the ancestor blocks that should point to our new block into
	// memory, and give them the pointer to the new block.
	pos, idx := bf.pos, bf.idx+1

	for i := 1; i < len(ptrs); i++ {
		if idx%(1<<uint(i)) != 0 {
			continue
		} else if err := bf.load(ptrs[i], pos-(1<<uint(i))*dataSize); err != nil {
			return 0, err
		}
		bf.curr.ptrs[i] = ptr
		if err := bf.persist(); err != nil {
			return 0, err
		}
	}

	// 'Load' the new block into memory.
	bf.pos = pos
	bf.idx = idx
	bf.ptr = ptr
	bf.curr = &block{ptrs: ptrs}

	return bf.writeAt(p, bf.pos)
}

func (bf *blockFile) writeAt(p []byte, offset int64) (int, error) {
	offset = offset - bf.idx*dataSize
	if offset == dataSize {
		return 0, errEndOfBlock
	} else if offset < 0 || offset > dataSize {
		return 0, errInvalidOffset
	}

	// Expand data slice if necessary.
	end := offset + int64(len(p))
	if end > dataSize {
		end = dataSize
	}
	if end > int64(len(bf.curr.data)) {
		temp := make([]byte, end)
		copy(temp, bf.curr.data)
		bf.curr.data = temp
	}

	n := copy(bf.curr.data[offset:], p)
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
	bf.pos = bf.pos / dataSize * dataSize

	// Follow the skiplist.
	if offset < bf.pos {
		if err := bf.load(bf.start, 0); err != nil {
			return -1, err
		}
	}

	for bf.pos != offset {
		// See if we have what we need in-memory.
		if d := offset - bf.pos; d < dataSize {
			bf.pos += d
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
			pos := bf.pos + (1<<uint(i))*dataSize
			if pos > offset {
				continue
			}

			// This pointer will get us as far as possible without going over.
			if err := bf.load(bf.curr.ptrs[i], pos); err != nil {
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
	ptrs []uint32 // ptrs contains the skiplist pointers from the current block.
	data []byte   // data is the block's application data.
}

func parseBlock(raw []byte) (*block, error) {
	if len(raw) != blockSize {
		return nil, fmt.Errorf("unexpected size: %v != %v", len(raw), blockSize)
	}

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

	return &block{ptrs, raw[:size]}
}

// Upgrade modifies this node from a tail node to an intermediate node and
// returns the pointers for the next tail node.
func (b *block) Upgrade(currIdx int64, currPtr, nextPtr uint32) []uint32 {
	// Compute the tail pointers for the subsequent block.
	out := make([]uint32, numPtrs)
	out[0] = nilPtr
	for i := 1; i < len(out); i++ {
		if currIdx%(1<<uint(i)) == 0 {
			out[i] = currPtr
		} else {
			out[i] = b.ptrs[i]
		}
	}

	// Update this block to point to the next block and nothing else, because
	// nothing else exists past that.
	b.ptrs[0] = nextPtr
	for i := 1; i < len(b.ptrs); i++ {
		b.ptrs[i] = nilPtr
	}

	return out
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
