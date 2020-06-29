package persistent

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"golang.org/x/crypto/argon2"
)

// treeHead is the authenticated head of the Merkle tree built over the user's
// data.
type treeHead struct {
	Version uint64 // Version is a counter of the number of modifications made to the tree.
	Nodes   uint64 // Nodes is the number of nodes in the tree / the maximum pointer plus one.
	Hash    []byte // Hash is the root of the Merkle tree.
	Tag     []byte // Tag is a MAC over all the information above.
}

func marshalTreeHead(head *treeHead, mac hash.Hash) ([]byte, error) {
	tag, err := head.expectedTag(mac)
	if err != nil {
		return nil, err
	}
	head.Tag = tag
	return json.Marshal(head)
}

func unmarshalTreeHead(raw []byte, mac hash.Hash) (*treeHead, error) {
	head := &treeHead{}
	if err := json.Unmarshal(raw, head); err != nil {
		return nil, err
	} else if err := head.validate(mac); err != nil {
		return nil, err
	}
	return head, nil
}

// readPinFile reads the pin file from disk as a starting point. Keeping a file
// on disk helps detect when there has been a malicious rollback or the state
// has been forked.
func readPinFile(pinFile string, mac hash.Hash) (*treeHead, error) {
	data, err := ioutil.ReadFile(pinFile)
	if os.IsNotExist(err) {
		log.Println("integrity: local pin file not found, will accept whatever remote storage returns")
		return &treeHead{}, nil
	} else if err != nil {
		return nil, err
	}
	return unmarshalTreeHead(data, mac)
}

// expectedTag returns the expected value of the `Tag` field.
func (th *treeHead) expectedTag(mac hash.Hash) ([]byte, error) {
	defer mac.Reset()

	if err := binary.Write(mac, binary.LittleEndian, th.Version); err != nil {
		return nil, err
	} else if err := binary.Write(mac, binary.LittleEndian, th.Nodes); err != nil {
		return nil, err
	} else if _, err := mac.Write(th.Hash); err != nil {
		return nil, err
	}

	return mac.Sum(nil), nil
}

// validate checks that the `Tag` field of `th` is correct.
func (th *treeHead) validate(mac hash.Hash) error {
	tag, err := th.expectedTag(mac)
	if err != nil {
		return err
	} else if !hmac.Equal(tag, th.Tag) {
		return fmt.Errorf("integrity: failed to validate tree head")
	}
	return nil
}

func (th *treeHead) clone() *treeHead {
	return &treeHead{
		Version: th.Version,
		Nodes:   th.Nodes,
		Hash:    dup(th.Hash),
		Tag:     dup(th.Tag),
	}
}

func (th *treeHead) equals(other *treeHead) bool {
	return th.Version == other.Version &&
		th.Nodes == other.Nodes &&
		bytes.Equal(th.Hash, other.Hash) &&
		bytes.Equal(th.Tag, other.Tag)
}

// dataPtr returns the pointer to the `ptr`-th data block. It adjusts `ptr` for
// the blocks of integrity-related metadata.
func dataPtr(ptr uint64) uint64 {
	offset := uint64(1) // The first block is the tree head.

	// Every 8 blocks we have 1 first-level block containing the hashes of the
	// previous 8 data blocks. Then every 64 blocks, we have 1 second-level
	// block containing the hashes of the previous 8 first-level blocks. And so
	// on...
	n := uint64(8)
	for level := uint64(0); level < 21; level++ {
		offset += ptr / n
		n = 8 * n
	}

	return ptr + offset
}

// checksumPtr returns the pointer to the checksum block at the given level in
// the tree, with the given offset from the left.
func checksumPtr(level int, offset uint64) uint64 {
	// Compute the pointer of the last data block within the subtree. The
	// integrity block is going to be `level`+1 blocks after that.
	nodesPerSubtree := uint64(1) << (3 * uint(level+1))
	lastBlock := dataPtr(nodesPerSubtree*(offset+1) - 1)

	return lastBlock + uint64(level) + 1
}

// checksumBlocks returns the path from the leaf data block at `ptr` to the root
// of the tree. Each element of the returned slice is one level: the first
// number is id of the checksum block within its level, and the second number is
// the id of the hash in the checksum block to check.
func checksumBlocks(ptr, nodes uint64) (out [][2]uint64) {
	max := nodes - 1

	for level := uint64(0); level < 21; level++ {
		out = append(out, [2]uint64{ptr / 8, ptr % 8})

		max = max / 8
		if max == 0 {
			break
		}
		ptr = ptr / 8
	}

	return out
}

func leafHash(data []byte) [32]byte {
	if data == nil {
		return [32]byte{}
	}
	return sha256.Sum256(append([]byte{0}, data...))
}

func intermediateHash(data []byte) [32]byte {
	return sha256.Sum256(append([]byte{1}, data...))
}

type integrity struct {
	base BlockStorage
	mac  hash.Hash

	pinned *treeHead
	curr   *treeHead

	pinFile  string
	lastSave time.Time
}

// WithIntegrity wraps a BlockStorage implementation and builds a Merkle tree
// over the data stored.
//
// The root of the Merkle tree is authenticated by `password`, and a copy of the
// root and other metadata is kept in `pinFile`.
func WithIntegrity(base BlockStorage, password, pinFile string) (BlockStorage, error) {
	// NOTE: The fixed salt to Argon2 is intentional. Its purpose is domain
	// separation, not to frustrate a password cracker.
	key := argon2.IDKey([]byte(password), []byte("534ffca65b68a9b3"), 1, 64*1024, 4, 32)
	mac := hmac.New(sha256.New, key)

	pinned, err := readPinFile(pinFile, mac)
	if err != nil {
		return nil, err
	}
	return &integrity{base, mac, pinned, nil, pinFile, time.Time{}}, nil
}

func (i *integrity) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	if len(prefetch) > 0 {
		return nil, fmt.Errorf("integrity: prefetch is not supported")
	}
	data, err := i.base.Start(ctx, []uint64{0})
	if err != nil {
		return nil, err
	}

	// Read the tree head from storage and validate it against the one we have
	// pinned.
	if data[0] == nil {
		i.pinned, i.curr = &treeHead{}, &treeHead{}
		return nil, nil
	} else if err != nil {
		i.Rollback(ctx)
		return nil, err
	}
	pinned, err := unmarshalTreeHead(data[0], i.mac)
	if err != nil {
		i.Rollback(ctx)
		return nil, err
	} else if pinned.Version < i.pinned.Version {
		i.Rollback(ctx)
		return nil, fmt.Errorf("integrity: tree head read from remote storage is older than expected")
	} else if pinned.Version == i.pinned.Version {
		if !bytes.Equal(pinned.Hash, i.pinned.Hash) {
			i.Rollback(ctx)
			return nil, fmt.Errorf("integrity: tree head read from remote storage has unexpected root hash")
		}
	}
	i.pinned, i.curr = pinned, pinned.clone()

	// If a new integrity pin hasn't been saved to disk in some time, do that.
	if time.Since(i.lastSave) > 10*time.Second {
		if err := os.MkdirAll(path.Dir(i.pinFile), 0744); err != nil {
			log.Printf("integrity: failed to create directory for pin file: %v", err)
		} else if err := ioutil.WriteFile(i.pinFile, data[0], 0744); err != nil {
			log.Printf("integrity: failed to write pin file: %v", err)
		} else {
			i.lastSave = time.Now()
		}
	}

	return nil, nil
}

func (i *integrity) getMeta(ptr uint64) (ptrs []uint64, checks [][2]uint64) {
	ptrs = []uint64{dataPtr(ptr)}

	checks = checksumBlocks(ptr, i.curr.Nodes)
	for level, check := range checks {
		ptrs = append(ptrs, checksumPtr(level, check[0]))
	}

	return ptrs, checks
}

func (i *integrity) validateGet(ptrs []uint64, checks [][2]uint64, data map[uint64][]byte) error {
	expected := leafHash(data[ptrs[0]])

	for level, check := range checks {
		block, ok := data[ptrs[level+1]]
		if !ok {
			return fmt.Errorf("integrity: missing checksum block")
		} else if len(block) != 8*32 {
			return fmt.Errorf("integrity: checksum block is malformed")
		} else if !bytes.Equal(expected[:], block[32*check[1]:32*check[1]+32]) {
			return fmt.Errorf("integrity: block does not equal expected value")
		}
		expected = intermediateHash(block)
	}

	if !bytes.Equal(expected[:], i.curr.Hash) {
		return fmt.Errorf("integrity: block does not equal tree head")
	}
	return nil
}

func (i *integrity) Get(ctx context.Context, ptr uint64) ([]byte, error) {
	data, err := i.GetMany(ctx, []uint64{ptr})
	if err != nil {
		return nil, err
	} else if data[ptr] == nil {
		return nil, ErrObjectNotFound
	}
	return data[ptr], nil
}

func (i *integrity) GetMany(ctx context.Context, ptrs []uint64) (map[uint64][]byte, error) {
	// Calculate the pointers to fetch and checks to perform for each Get.
	ptrRef := make([]uint64, 0, len(ptrs))
	allPtrs := make([][]uint64, 0, len(ptrs))
	allChecks := make([][][2]uint64, 0, len(ptrs))

	for _, ptr := range ptrs {
		if ptr >= i.curr.Nodes {
			continue
		}
		ptrs1, checks1 := i.getMeta(ptr)

		ptrRef = append(ptrRef, ptr)
		allPtrs = append(allPtrs, ptrs1)
		allChecks = append(allChecks, checks1)
	}

	// Construct the de-duplicated set of pointers to request.
	dedupPtrs := make(map[uint64]struct{})
	for _, ptrs1 := range allPtrs {
		for _, ptr := range ptrs1 {
			dedupPtrs[ptr] = struct{}{}
		}
	}

	finalPtrs := make([]uint64, 0, len(dedupPtrs))
	for ptr, _ := range dedupPtrs {
		finalPtrs = append(finalPtrs, ptr)
	}

	// Actually fetch the data we need from the backend.
	data, err := i.base.GetMany(ctx, finalPtrs)
	if err != nil {
		return nil, err
	}

	// Validate all data fetched.
	for j, _ := range ptrRef {
		if err := i.validateGet(allPtrs[j], allChecks[j], data); err != nil {
			return nil, err
		}
	}

	// Construct output map.
	out := make(map[uint64][]byte)
	for i, ptr := range ptrRef {
		d, ok := data[allPtrs[i][0]]
		if ok {
			out[ptr] = d
		}
	}
	return out, nil
}

func (i *integrity) createChecksumBlocks(ctx context.Context, prev, curr uint64) error {
	curr2 := curr

	expectedLeft := [32]byte{} // The expected value of left-most block of level.
	copy(expectedLeft[:], i.curr.Hash)
	expectedRest := [32]byte{} // The expected value of every other block of level.

	for level := 0; level < 21; level++ {
		if prev == 1 && level > 0 {
			prev = 0
		} else {
			prev = (prev + 7) / 8
		}
		if curr == 1 && level > 0 {
			curr = 0
		} else {
			curr = (curr + 7) / 8
		}

		// Compute the contents of the left-most block of the level (if we
		// happen to need to set that block), and the contents of every other
		// block.
		dataLeft, dataRest := make([]byte, 8*32), make([]byte, 8*32)
		for i := 0; i < 8; i++ {
			copy(dataLeft[32*i:], expectedRest[:])
			copy(dataRest[32*i:], expectedRest[:])
		}
		copy(dataLeft[0:], expectedLeft[:])

		// Update the expected value for the next level.
		expectedRest = intermediateHash(dataRest)

		// Write the new checksum blocks.
		for offset := prev; offset < curr; offset++ {
			if offset == 0 {
				if err := i.base.Set(ctx, checksumPtr(level, offset), dataLeft, Metadata); err != nil {
					return err
				}
				// Only update this value when we consume it, since we took the
				// tree head and that's already several layers up the tree.
				expectedLeft = intermediateHash(dataLeft)
			} else {
				if err := i.base.Set(ctx, checksumPtr(level, offset), dataRest, Metadata); err != nil {
					return err
				}
			}
		}
	}

	i.curr.Nodes = curr2
	i.curr.Hash = expectedLeft[:]
	return nil
}

func (i *integrity) Set(ctx context.Context, ptr uint64, data []byte, dt DataType) error {
	if ptr+1 > i.curr.Nodes {
		if err := i.createChecksumBlocks(ctx, i.curr.Nodes, ptr+1); err != nil {
			return err
		}
	}
	if err := i.base.Set(ctx, dataPtr(ptr), data, dt); err != nil {
		return err
	}

	ptrs := make([]uint64, 0)
	checks := checksumBlocks(ptr, i.curr.Nodes)
	for level, check := range checks {
		ptrs = append(ptrs, checksumPtr(level, check[0]))
	}

	nodes, err := i.base.GetMany(ctx, ptrs)
	if err != nil {
		return err
	}

	prev, expected := [32]byte{}, leafHash(data)
	for level, check := range checks {
		block, ok := nodes[ptrs[level]]
		if !ok {
			return fmt.Errorf("integrity: missing checksum block")
		} else if len(block) != 8*32 {
			return fmt.Errorf("integrity: checksum block is malformed")
		} else if level > 0 && !bytes.Equal(prev[:], block[32*check[1]:32*check[1]+32]) {
			return fmt.Errorf("integrity: block does not equal expected value")
		}
		prev = intermediateHash(block)

		copy(block[32*check[1]:], expected[:])
		if err := i.base.Set(ctx, ptrs[level], block, Metadata); err != nil {
			return err
		}
		expected = intermediateHash(block)
	}

	if !bytes.Equal(prev[:], i.curr.Hash) {
		return fmt.Errorf("integrity: block does not equal tree head")
	}
	i.curr.Version += 1
	i.curr.Hash = expected[:]

	return nil
}

func (i *integrity) Commit(ctx context.Context) error {
	// Write the new tree head to storage and commit the transaction.
	data, err := marshalTreeHead(i.curr, i.mac)
	if err != nil {
		return err
	} else if err := i.base.Set(ctx, 0, data, Metadata); err != nil {
		return err
	} else if err := i.base.Commit(ctx); err != nil {
		return err
	}

	// Write the new tree head to disk as well, but fail-open if it doesn't work
	// because the transaction is already committed.
	if err := os.MkdirAll(path.Dir(i.pinFile), 0744); err != nil {
		log.Printf("integrity: failed to create directory for pin file: %v", err)
	} else if err := ioutil.WriteFile(i.pinFile, data, 0744); err != nil {
		log.Printf("integrity: failed to write pin file: %v", err)
	} else {
		i.lastSave = time.Now()
	}

	return nil
}

func (i *integrity) Rollback(ctx context.Context) {
	i.base.Rollback(ctx)
	i.curr = nil
}
