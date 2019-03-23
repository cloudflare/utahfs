package utahfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	nilPtr = ^uint32(0)

	numPtrs  = 12
	dataSize = 32 * 1024

	blockSize = 4*numPtrs + dataSize
)

// blockStorage implements large files over fixed-size blocks provided by an
// object storage service.
type blockStorage struct {
	store ObjectStorage
	trash uint32
}

// func (bs *blockStorage) Open(ptr uint64)

// blockFile implements read-write functionality for a variable-size file over
// fixed-size blocks.
//
// Internally, it has the structure of a skip list. Blocks are prefixed with
// pointers to subsequent blocks, where each pointer is exponentially further
// away.
type blockFile struct {
	parent *blockStorage

	// start points to the first block of the file.
	start uint32
	// pos is our current position in the file, in bytes.
	pos int64
	// size is the total size of the file, in bytes.
	size int64

	// ptrs contains the skiplist pointers from the current block.
	ptrs []uint32
	// buff is unread data from the current block.
	buff *bytes.Buffer
}

// load pulls the block at `ptr` into memory. `pos` is our new position in the
// file.
func (bf *blockFile) load(ptr uint32, pos int64) error {
	data, err := bf.parent.store.Get(fmt.Sprintf("%x", pos))
	if err != nil {
		return fmt.Errorf("failed to load block %x: %v", ptr, err)
	} else if len(data) != blockSize {
		return fmt.Errorf("block %v has unexpected size: %v != %v", ptr, len(data), blockSize)
	}
	buff := bytes.NewBuffer(data)

	// Read pointers.
	ptrs := make([]uint32, numPtrs)
	for i := 0; i < len(ptrs); i++ {
		if err := binary.Read(buff, binary.LittleEndian, &ptrs[i]); err != nil {
			return err
		}
	}

	// Operation was successful, expose data to other methods.
	bf.pos = pos
	bf.ptrs = ptrs
	bf.buff = buff

	return nil
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
		// See if we have what we need in-memory.
		if d := offset - bf.pos; d < int64(bf.buff.Len()) {
			bf.buff.Next(int(d))
			return offset, nil
		}

		// We need to load another block. Choose the next pointer to follow.
		stepped := false
		for i := len(bf.ptrs) - 1; i >= 0; i-- {
			if bf.ptrs[i] == nilPtr {
				continue
			}
			pos := (1<<uint(i))*dataSize + bf.pos
			if pos > offset {
				continue
			}

			// This pointer will get us as far as possible without going over.
			if err := bf.load(bf.ptrs[i], pos); err != nil {
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
