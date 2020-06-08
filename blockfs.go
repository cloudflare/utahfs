package utahfs

import (
	"context"
	"fmt"
	"io"

	"github.com/cloudflare/utahfs/persistent"
)

const nilPtr = ^uint64(0)

var errEndOfBlock = fmt.Errorf("blockfs: reached end of block")

func p(ptr uint64) uint64 { return 2 * ptr }
func d(ptr uint64) uint64 { return 2*ptr + 1 }

// BlockFilesystem implements large files as skiplists over fixed-size blocks
// stored in an object storage service.
type BlockFilesystem struct {
	store *persistent.AppStorage

	numPtrs   int64
	dataSize  int64
	splitPtrs bool
}

// NewBlockFilesystem returns a new block-based filesystem. Blocks will have
// `numPtrs` pointers in their skiplist and contain at most `dataSize` bytes of
// application data.
//
// Recommended values:
//   numPtrs = 12, dataSize = 32*1024
//
// This system manages two pieces of global state:
//   1. trash - Points to the first block of the trash list: a linked list of
//      blocks which have been discarded and are free for re-allocation.
//   2. next - The next unallocated pointer. A block with this pointer is
//      created only if the trash list is empty.
//
// `splitPtrs` is true if the pointers section of a block should be stored
// separately from the data section, and false if they should be stored
// together. Storing them separately can improve seek performance.
func NewBlockFilesystem(store *persistent.AppStorage, numPtrs, dataSize int64, splitPtrs bool) (*BlockFilesystem, error) {
	if numPtrs < 1 {
		return nil, fmt.Errorf("blockfs: number of pointers must be greater than zero")
	} else if dataSize < 1 || dataSize >= (1<<24) {
		return nil, fmt.Errorf("blockfs: size of data block must be greater zero and less than %v", 1<<24)
	}

	return &BlockFilesystem{
		store: store,

		numPtrs:   numPtrs,
		dataSize:  dataSize,
		splitPtrs: splitPtrs,
	}, nil
}

func (bfs *BlockFilesystem) blockSize() int64     { return bfs.blockPtrsSize() + bfs.blockDataSize() }
func (bfs *BlockFilesystem) blockPtrsSize() int64 { return 8 * bfs.numPtrs }
func (bfs *BlockFilesystem) blockDataSize() int64 { return 3 + bfs.dataSize }

// allocate returns the pointer of a block which is free for use by the caller.
func (bfs *BlockFilesystem) allocate(ctx context.Context) (uint64, error) {
	state, err := bfs.store.State(ctx)
	if err != nil {
		return nilPtr, err
	} else if state.TrashPtr == nilPtr {
		next := state.NextPtr
		state.NextPtr += 1
		return next, nil
	}

	b := &block{parent: bfs}
	if bfs.splitPtrs {
		rawPtrs, err := bfs.store.Get(ctx, p(state.TrashPtr))
		if err != nil {
			return nilPtr, err
		} else if err := b.UnmarshalPtrs(rawPtrs); err != nil {
			return nilPtr, fmt.Errorf("blockfs: failed to parse block %x: %v", state.TrashPtr, err)
		}
	} else {
		raw, err := bfs.store.Get(ctx, state.TrashPtr)
		if err != nil {
			return nilPtr, err
		} else if err := b.Unmarshal(raw); err != nil {
			return nilPtr, fmt.Errorf("blockfs: failed to parse block %x: %v", state.TrashPtr, err)
		}
	}

	trash := state.TrashPtr
	state.TrashPtr = b.ptrs[0]
	return trash, nil
}

// Create creates a new file. It returns the pointer to the file and an open
// copy.
func (bfs *BlockFilesystem) Create(ctx context.Context, dt persistent.DataType) (uint64, *BlockFile, error) {
	ptr, err := bfs.allocate(ctx)
	if err != nil {
		return nilPtr, nil, err
	}

	ptrs := make([]uint64, bfs.numPtrs)
	ptrs[0] = nilPtr
	for i := 1; i < len(ptrs); i++ {
		ptrs[i] = ptr
	}

	bf := &BlockFile{
		parent: bfs,
		ctx:    ctx,

		start: ptr,
		size:  0,
		dt:    dt,

		pos:  0,
		idx:  0,
		ptr:  ptr,
		curr: &block{parent: bfs, ptrs: ptrs, data: make([]byte, 0)},
	}
	if err := bf.persist(); err != nil {
		return nilPtr, nil, err
	}

	return ptr, bf, nil
}

// Open returns a handle to an existing file.
func (bfs *BlockFilesystem) Open(ctx context.Context, ptr uint64, dt persistent.DataType) (*BlockFile, error) {
	bf := &BlockFile{
		parent: bfs,
		ctx:    ctx,

		start: ptr,
		size:  0,
		dt:    dt,
	}
	if err := bf.load(ptr, 0, false); err != nil {
		return nil, err
	}

	return bf, nil
}

// Unlink allows the blocks allocated for a file to be re-used for other
// purposes.
func (bfs *BlockFilesystem) Unlink(ctx context.Context, ptr uint64) error {
	bf, err := bfs.Open(ctx, ptr, persistent.Unknown)
	if err != nil {
		return err
	}

	// Seek to the end of the skiplist at `ptr`
	for {
		if bf.curr.ptrs[0] == nilPtr {
			break
		}

		stepped := false
		for i := len(bf.curr.ptrs) - 1; i >= 0; i-- {
			if bf.curr.ptrs[i] == nilPtr {
				continue
			} else if err := bf.load(bf.curr.ptrs[i], 0, false); err != nil {
				return err
			}
			stepped = true
			break
		}
		if !stepped { // This error should only ever occur if the skiplist is corrupted.
			return fmt.Errorf("blockfs: failed to find a suitable pointer in skiplist")
		}
	}

	// Prepend the trash list with `bf` by setting the tail pointer of `bf` as
	// the current value of `trash` and updating `trash` to be the head of `bf`.
	state, err := bfs.store.State(ctx)
	if err != nil {
		return err
	}
	bf.curr.ptrs[0] = state.TrashPtr
	state.TrashPtr = ptr

	return bf.persist()
}

// BlockFile implements read-write functionality for a variable-size file over
// a skiplist of fixed-size blocks.
type BlockFile struct {
	parent *BlockFilesystem
	ctx    context.Context

	// start points to the first block of the file.
	start uint64
	// size is the total size of the file, in bytes.
	size int64
	// dt is the type of data contained in this file
	dt persistent.DataType

	// pos is our current position in the file, in bytes.
	pos int64
	// idx is the index of this block in the skiplist.
	idx int64
	// ptr is the pointer for the current block of the file.
	ptr uint64
	// curr is the parsed version of the current block.
	curr *block
}

// persist saves any changes to the current block to the storage backend.
func (bf *BlockFile) persist() error {
	if bf.parent.splitPtrs {
		err := bf.parent.store.Set(bf.ctx, p(bf.ptr), bf.curr.MarshalPtrs(), persistent.Metadata)
		if err != nil {
			return err
		} else if bf.curr.data != nil {
			err := bf.parent.store.Set(bf.ctx, d(bf.ptr), bf.curr.MarshalData(), bf.dt)
			if err != nil {
				return err
			}
		}
	} else {
		err := bf.parent.store.Set(bf.ctx, bf.ptr, bf.curr.Marshal(), bf.dt)
		if err != nil {
			return err
		}
	}
	return nil
}

// load pulls the block at `ptr` into memory. `pos` is our new position in the
// file.
func (bf *BlockFile) load(ptr uint64, pos int64, data bool) error { // NOTE: Don't load ptrs twice.
	curr := &block{parent: bf.parent}

	if bf.parent.splitPtrs {
		ptrPtr, dataPtr := p(ptr), d(ptr)
		ptrs := []uint64{ptrPtr}
		if data {
			ptrs = append(ptrs, dataPtr)
		}

		raw, err := bf.parent.store.GetMany(bf.ctx, ptrs)
		if err != nil {
			return err
		}
		for _, ptr := range ptrs {
			if raw[ptr] == nil {
				return persistent.ErrObjectNotFound
			}
		}

		if err := curr.UnmarshalPtrs(raw[ptrPtr]); err != nil {
			return fmt.Errorf("blockfs: failed to parse block %x: %v", ptr, err)
		} else if data {
			if err := curr.UnmarshalData(raw[dataPtr]); err != nil {
				return fmt.Errorf("blockfs: failed to parse block %x: %v", ptr, err)
			}
		}
	} else {
		raw, err := bf.parent.store.Get(bf.ctx, ptr)
		if err != nil {
			return err
		} else if err := curr.Unmarshal(raw); err != nil {
			return fmt.Errorf("blockfs: failed to parse block %x: %v", ptr, err)
		}
	}

	bf.pos = pos
	bf.idx = pos / bf.parent.dataSize
	bf.ptr = ptr
	bf.curr = curr

	return nil
}

func (bf *BlockFile) Read(p []byte) (int, error) {
	n, err := bf.read(p)
	bf.pos += int64(n)
	return n, err
}

func (bf *BlockFile) read(p []byte) (int, error) {
	n, err := bf.readAt(p, bf.pos)
	if err == errEndOfBlock {
		if bf.curr.ptrs[0] == nilPtr {
			return 0, io.EOF
		} else if err := bf.load(bf.curr.ptrs[0], bf.pos, true); err != nil {
			return 0, err
		}
		return bf.readAt(p, bf.pos)
	}

	return n, err
}

func (bf *BlockFile) readAt(p []byte, offset int64) (int, error) {
	offset = offset - bf.idx*bf.parent.dataSize
	if offset == bf.parent.dataSize {
		return 0, errEndOfBlock
	} else if offset < 0 || offset > bf.parent.dataSize {
		return 0, fmt.Errorf("blockfs: invalid offset to read from block")
	} else if bf.curr.data == nil { // Load the block data if it hasn't been already.
		if err := bf.load(bf.ptr, bf.pos, true); err != nil {
			return 0, err
		}
	}

	if offset >= int64(len(bf.curr.data)) {
		return 0, io.EOF
	}
	n := copy(p, bf.curr.data[offset:])
	return n, nil
}

func (bf *BlockFile) Write(p []byte) (int, error) {
	n := 0

	for first := true; n < len(p); first = false {
		m, err := bf.write(first, p[n:])

		n += m
		bf.pos += int64(m)
		if bf.pos > bf.size {
			bf.size = bf.pos
		}

		if err != nil {
			return n, err
		}
	}

	return n, bf.persist()
}

func (bf *BlockFile) write(first bool, p []byte) (int, error) {
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
		} else if err := bf.load(bf.curr.ptrs[0], bf.pos, true); err != nil {
			return 0, err
		}
		return bf.writeAt(p, bf.pos)
	}

	// There is no next block. We have to create it. First thing is to change
	// the format of the current block from a tail to an intermediate.
	ptr, err := bf.parent.allocate(bf.ctx)
	if err != nil {
		return 0, err
	}
	if first && bf.parent.splitPtrs {
		bf.curr.data = nil
	}
	ptrs := bf.curr.Upgrade(bf.idx, bf.ptr, ptr) // NOTE: Are we just changing ptrs here? Unload data if so?
	if err := bf.persist(); err != nil {
		return 0, err
	}

	// Load all the ancestor blocks that should point to our new block into
	// memory, and give them the pointer to the new block.
	pos, idx := bf.pos, bf.idx+1

	for i := 1; i < len(ptrs); i++ {
		if idx%(1<<uint(i)) != 0 {
			continue
		} else if err := bf.load(ptrs[i], pos-(1<<uint(i))*bf.parent.dataSize, false); err != nil {
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
	bf.curr = &block{parent: bf.parent, ptrs: ptrs, data: make([]byte, 0)}

	return bf.writeAt(p, bf.pos)
}

func (bf *BlockFile) writeAt(p []byte, offset int64) (int, error) {
	offset = offset - bf.idx*bf.parent.dataSize
	if offset == bf.parent.dataSize {
		return 0, errEndOfBlock
	} else if offset < 0 || offset > bf.parent.dataSize {
		return 0, fmt.Errorf("blockfs: invalid offset to write to block")
	} else if bf.curr.data == nil { // Load the block data if it hasn't been already.
		if err := bf.load(bf.ptr, bf.pos, true); err != nil {
			return 0, err
		}
	}

	// Expand data slice if necessary.
	end := offset + int64(len(p))
	if end > bf.parent.dataSize {
		end = bf.parent.dataSize
	}
	if end > int64(len(bf.curr.data)) {
		temp := make([]byte, end)
		copy(temp, bf.curr.data)
		bf.curr.data = temp
	}

	n := copy(bf.curr.data[offset:], p)
	return n, nil
}

func (bf *BlockFile) Seek(offset int64, whence int) (int64, error) {
	// Calculate offset relative to the beginning of the file.
	if whence == io.SeekStart {
		// offset = offset
	} else if whence == io.SeekCurrent {
		offset = bf.pos + offset
	} else if whence == io.SeekEnd {
		offset = bf.size + offset
	} else {
		return -1, fmt.Errorf("blockfs: unexpected value for whence")
	}

	if offset < 0 {
		return -1, fmt.Errorf("blockfs: cannot seek past beginning of file")
	} else if offset > bf.size {
		return -1, fmt.Errorf("blockfs: cannot seek past end of file")
	}
	bf.pos = bf.idx * bf.parent.dataSize

	// Follow the skiplist.
	if offset < bf.pos {
		if err := bf.load(bf.start, 0, false); err != nil {
			return -1, err
		}
	}

	for bf.pos != offset {
		// See if we have what we need in-memory.
		if d := offset - bf.pos; d < bf.parent.dataSize {
			bf.pos += d
			return offset, nil
		} else if bf.curr.ptrs[0] == nilPtr && d == bf.parent.dataSize {
			bf.pos += d
			return offset, nil
		}

		// We need to load another block. Choose the next pointer to follow.
		if bf.curr.ptrs[0] == nilPtr {
			return -1, fmt.Errorf("blockfs: unexpectedly reached end of skiplist")
		}

		stepped := false
		for i := len(bf.curr.ptrs) - 1; i >= 0; i-- {
			if bf.curr.ptrs[i] == nilPtr {
				continue
			}
			pos := bf.pos + (1<<uint(i))*bf.parent.dataSize
			if pos > offset {
				continue
			}

			// This pointer will get us as far as possible without going over.
			if err := bf.load(bf.curr.ptrs[i], pos, false); err != nil {
				return -1, err
			}
			stepped = true
			break
		}
		if !stepped { // This error should only ever occur if the skiplist is corrupted.
			return -1, fmt.Errorf("blockfs: failed to find a suitable pointer in skiplist")
		}
	}

	return bf.pos, nil
}

func (bf *BlockFile) Truncate(size int64) error {
	if size < 0 {
		return fmt.Errorf("blockfs: cannot truncate to negative size")
	} else if size >= bf.size {
		_, err := bf.Seek(0, io.SeekEnd)
		return err
	}
	bf.size = size

	// Seek to any blocks that might point past the end of the new file
	// boundary. Update them to no longer point over, and collect their pointers
	// for the new tail block.
	tailPtrs := make([]uint64, bf.parent.numPtrs)
	tailPtrs[0] = nilPtr

	endIdx := (bf.size - 1) / bf.parent.dataSize
	for i := len(tailPtrs) - 1; i >= 1; i-- {
		jump := int64(1) << uint(i)
		idx := endIdx / jump * jump

		if _, err := bf.Seek(idx*bf.parent.dataSize, io.SeekStart); err != nil {
			return err
		}
		tailPtrs[i] = bf.ptr

		// Clear any pointers that would now go past the end of the file.
		if idx == endIdx {
			continue
		}
		for j := i; j < len(bf.curr.ptrs); j++ {
			bf.curr.ptrs[j] = nilPtr
		}
		if err := bf.persist(); err != nil {
			return err
		}
	}

	// Move the rest of the file to the trash.
	if _, err := bf.Seek(endIdx*bf.parent.dataSize, io.SeekStart); err != nil {
		return err
	} else if bf.curr.ptrs[0] != nilPtr {
		if err := bf.parent.Unlink(bf.ctx, bf.curr.ptrs[0]); err != nil {
			return err
		}
	}

	// Convert this block into a tail block.
	if err := bf.load(bf.ptr, bf.pos, true); err != nil {
		return err
	}
	bf.curr.ptrs = tailPtrs
	bf.curr.data = bf.curr.data[:bf.size-endIdx*bf.parent.dataSize]
	if err := bf.persist(); err != nil {
		return err
	}
	bf.pos += int64(len(bf.curr.data))

	return nil
}

type block struct {
	parent *BlockFilesystem

	ptrs []uint64 // ptrs contains the skiplist pointers from the current block.
	data []byte   // data is the block's application data.
}

// Upgrade modifies this block from a tail into an intermediate and returns the
// pointers for the next tail.
func (b *block) Upgrade(currIdx int64, currPtr, nextPtr uint64) []uint64 {
	// Compute the tail pointers for the subsequent block.
	out := make([]uint64, b.parent.numPtrs)
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
	return append(b.MarshalPtrs(), b.MarshalData()...)
}

func (b *block) MarshalPtrs() []byte {
	out := make([]byte, b.parent.blockPtrsSize())
	rest := out[0:]

	for i := 0; i < len(b.ptrs); i++ {
		writeInt(int(b.ptrs[i]), rest[:8])
		rest = rest[8:]
	}

	return out
}

func (b *block) MarshalData() []byte {
	out := make([]byte, b.parent.blockDataSize())
	rest := out[0:]

	// Write length.
	writeInt(len(b.data), rest[:3])
	rest = rest[3:]

	// Write data.
	copy(rest, b.data)

	return out
}

func (b *block) Unmarshal(raw []byte) error {
	if int64(len(raw)) != b.parent.blockSize() {
		return fmt.Errorf("blockfs: unexpected size: %v != %v", len(raw), b.parent.blockSize())
	}
	if err := b.UnmarshalPtrs(raw[:b.parent.blockPtrsSize()]); err != nil {
		return err
	}
	return b.UnmarshalData(raw[b.parent.blockPtrsSize():])
}

func (b *block) UnmarshalPtrs(raw []byte) error {
	if int64(len(raw)) != b.parent.blockPtrsSize() {
		return fmt.Errorf("blockfs: unexpected size: %v != %v", len(raw), b.parent.blockPtrsSize())
	}

	b.ptrs = make([]uint64, b.parent.numPtrs)
	for i := 0; i < len(b.ptrs); i++ {
		b.ptrs[i] = uint64(readInt(raw[:8]))
		raw = raw[8:]
	}

	return nil
}

func (b *block) UnmarshalData(raw []byte) error {
	if int64(len(raw)) != b.parent.blockDataSize() {
		return fmt.Errorf("blockfs: unexpected size: %v != %v", len(raw), b.parent.blockDataSize())
	}

	size := readInt(raw[:3])
	raw = raw[3:]
	if len(raw) < size {
		return fmt.Errorf("blockfs: application data has unexpected size")
	}
	b.data = raw[:size]

	return nil
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
