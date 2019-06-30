package persistent

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io/ioutil"
	"log"
	"os"

	"golang.org/x/crypto/pbkdf2"
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
	for i := uint64(0); i < 21; i++ {
		offset += ptr / n
		n = 8 * n
	}

	return ptr + offset
}

// checkBlocks returns the path from the leaf data block at `ptr` to the root of
// the tree. Each element of the returned slice is one level: the first number
// is the pointer of the checksum block, and the second number is the hash in
// the checksum block to check.
func checkBlocks(ptr, nodes uint64) [][2]uint64 {
	max := nodes - 1
	checks := make([][2]uint64, 0)

	n := uint64(8)
	for i := uint64(0); i < 21; i++ {
		block := n*(ptr/8) + n + i + 1
		checks = append(checks, [2]uint64{block, ptr % 8})

		max = max / 8
		if max == 0 {
			break
		}
		ptr = ptr / 8
		n = 8 * n
	}

	return checks
}

func leafHash(data []byte) [32]byte {
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

	pinFile string
}

// WithIntegrity wraps a BlockStorage implementation and builds a Merkle tree
// over the data stored.
//
// The root of the Merkle tree is authenticated by `password`, and a copy of the
// root and other metadata is kept in `pinFile`.
func WithIntegrity(base BlockStorage, password, pinFile string) (BlockStorage, error) {
	key := pbkdf2.Key([]byte(password), []byte("534ffca65b68a9b3"), 4096, 32, sha1.New)
	mac := hmac.New(sha256.New, key)

	pinned, err := readPinFile(pinFile, mac)
	if err != nil {
		return nil, err
	}

	return &integrity{base, mac, pinned, nil, pinFile}, nil
}

func (i *integrity) Start(ctx context.Context) error {
	if err := i.base.Start(ctx); err != nil {
		return err
	}

	// Read the tree head from storage and validate it against the one we have
	// pinned.
	data, err := i.base.Get(ctx, 0)
	if err == ErrObjectNotFound {
		i.pinned, i.curr = &treeHead{}, &treeHead{}
		return nil
	} else if err != nil {
		return err
	}
	pinned, err := unmarshalTreeHead(data, i.mac)
	if err != nil {
		return err
	} else if pinned.Version < i.pinned.Version {
		i.base.Rollback(ctx)
		return fmt.Errorf("integrity: tree head read from remote storage is older than expected")
	} else if pinned.Version == i.pinned.Version {
		if !bytes.Equal(pinned.Hash, i.pinned.Hash) {
			i.base.Rollback(ctx)
			return fmt.Errorf("integrity: tree head read from remote storage has unexpected root hash")
		}
	}
	i.pinned, i.curr = pinned, pinned.clone()

	return nil
}

func (i *integrity) Get(ctx context.Context, ptr uint64) ([]byte, error) {
	if ptr >= i.curr.Nodes {
		return nil, ErrObjectNotFound
	}
	data, err := i.base.Get(ctx, dataPtr(ptr))
	if err != nil {
		return nil, err
	}
	expected := leafHash(data)

	for _, check := range checkBlocks(ptr, i.curr.Nodes) {
		block, err := i.base.Get(ctx, check[0])
		if err == ErrObjectNotFound {
			block = make([]byte, 8*32)
		} else if err != nil {
			return nil, err
		} else if len(block) != 8*32 {
			return nil, fmt.Errorf("integrity: checksum block is malformed")
		}
		if !bytes.Equal(expected[:], block[32*check[1]:32*check[1]+32]) {
			return nil, fmt.Errorf("integrity: block does not equal expected value")
		}
		expected = intermediateHash(block)
	}

	if !bytes.Equal(expected[:], i.curr.Hash) {
		return nil, fmt.Errorf("integrity: block does not equal tree head")
	}

	return data, nil
}

func (i *integrity) Set(ctx context.Context, ptr uint64, data []byte) error {
	if ptr+1 > i.curr.Nodes {
		i.curr.Nodes = ptr + 1
	}
	if err := i.base.Set(ctx, dataPtr(ptr), data); err != nil {
		return err
	}
	expected := leafHash(data)

	for _, check := range checkBlocks(ptr, i.curr.Nodes) {
		block, err := i.base.Get(ctx, check[0])
		if err == ErrObjectNotFound {
			block = make([]byte, 8*32)
		} else if err != nil {
			return err
		} else if len(block) != 8*32 {
			return fmt.Errorf("integrity: checksum block is malformed")
		}
		copy(block[32*check[1]:], expected[:])
		if err := i.base.Set(ctx, check[0], block); err != nil {
			return err
		}
		expected = intermediateHash(block)
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
	} else if err := i.base.Set(ctx, 0, data); err != nil {
		return err
	} else if err := i.base.Commit(ctx); err != nil {
		return err
	}

	// Write the new tree head to disk as well, but fail-open if it doesn't work
	// because the transaction is already committed.
	if err = ioutil.WriteFile(i.pinFile, data, 0744); err != nil {
		log.Printf("integrity: failed to write pin file: %v", err)
	}

	return nil
}

func (i *integrity) Rollback(ctx context.Context) {
	i.base.Rollback(ctx)
	i.curr = nil
}
