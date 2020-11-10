package persistent

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
)

const (
	// blockSize is the number of entries that go in a single block.
	blockSize int = 4
)

func marshalBucket(items map[uint64][]byte, maxSize int64) []byte {
	if len(items) > blockSize {
		panic("cannot marshal a bucket that has more than the max number of items")
	} else if maxSize>>32 != 0 {
		panic("max data size cannot be larger than 32 bits")
	}
	targetSize := blockSize * (8 + 4 + int(maxSize))

	buff := new(bytes.Buffer)
	buff.Grow(targetSize)

	for ptr, data := range items {
		if int64(len(data)) > maxSize {
			panic("data in bucket is larger than max size")
		}

		if err := binary.Write(buff, binary.LittleEndian, ptr); err != nil { // Pointer.
			panic(err)
		}
		if err := binary.Write(buff, binary.LittleEndian, uint32(len(data))); err != nil { // Data length.
			panic(err)
		}
		if _, err := buff.Write(data); err != nil { // Data.
			panic(err)
		}
		if _, err := buff.Write(make([]byte, maxSize-int64(len(data)))); err != nil { // Zero padding.
			panic(err)
		}
	}
	for i := len(items); i < blockSize; i++ {
		if err := binary.Write(buff, binary.LittleEndian, ^uint64(0)); err != nil { // Null pointer.
			panic(err)
		}
		if _, err := buff.Write(make([]byte, 4+maxSize)); err != nil { // Zero data length + zero padding.
			panic(err)
		}
	}

	if buff.Len() != targetSize {
		panic("unknown internal error occurred while marshaling block")
	}
	return buff.Bytes()
}

func unmarshalBucket(data []byte, maxSize int64) (map[uint64][]byte, error) {
	out := make(map[uint64][]byte)

	r := bytes.NewReader(data)
	for {
		// Pointer.
		var ptr uint64
		if err := binary.Read(r, binary.LittleEndian, &ptr); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, err
		}

		// Data length.
		var dataLen uint32
		if err := binary.Read(r, binary.LittleEndian, &dataLen); err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		} else if err != nil {
			return nil, err
		} else if int64(dataLen) > maxSize {
			return nil, fmt.Errorf("data in block is larger than max size")
		}

		// Data.
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}

		// Zero padding.
		padding := make([]byte, maxSize-int64(dataLen))
		if _, err := io.ReadFull(r, padding); err != nil {
			return nil, err
		}
		for _, b := range padding {
			if b != 0x00 {
				return nil, fmt.Errorf("padding bytes were not all zero")
			}
		}

		if ptr == ^uint64(0) {
			continue
		}
		out[ptr] = data
	}
}

type oblivious struct {
	base    BlockStorage
	store   *obliviousStore
	maxSize int64

	integ *integrity

	// needRollback is set to true when an error condition has occurred while
	// performing ORAM operations and it's safest to lose some privacy
	// guarantees and just start over.
	needRollback bool
	// originalVals is a map from a pointer to the original value of that
	// pointer before any Sets were applied.
	originalVals map[uint64][]byte
	// rollbackWrites are writes that we should try to apply in the event of a
	// rollback, assuming there have been no other error conditions.
	rollbackWrites map[uint64][]byte
}

// WithORAM wraps a BlockStorage implementation and prevents outsiders from
// seeing the user's access pattern. This includes which data a user is
// accessing and whether the user is reading or writing, but does not include
// the total amount of accesses or when.
//
// A small amount of local data is stored in `store`. `maxSize` is the maximum
// size of a block of data.
func WithORAM(base BlockStorage, store ObliviousStorage, maxSize int64) (BlockStorage, error) {
	// Extract the integrity layer so we have access to the current version of
	// the corpus.
	enc, ok := unwrapAuditor(base).(*encryption)
	if !ok {
		return nil, fmt.Errorf("oblivious: expected encryption layer as input, but got: %T", base)
	}
	integ, ok := enc.base.(*integrity)
	if !ok {
		return nil, fmt.Errorf("oblivious: expected integrity layer below encryption layer, but got: %T", enc.base)
	}

	return &oblivious{
		base:    base,
		store:   newObliviousStore(store),
		maxSize: maxSize,

		integ: integ,
	}, nil
}

func (o *oblivious) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	if _, err := o.base.Start(ctx, nil); err != nil {
		return nil, err
	} else if err := o.store.Start(ctx, o.integ.curr.Version); err != nil {
		o.base.Rollback(ctx)
		return nil, err
	}

	o.needRollback = false
	o.originalVals = make(map[uint64][]byte)
	o.rollbackWrites = make(map[uint64][]byte)

	return o.GetMany(ctx, prefetch)
}

func (o *oblivious) Get(ctx context.Context, ptr uint64) ([]byte, error) {
	data, err := o.GetMany(ctx, []uint64{ptr})
	if err != nil {
		return nil, err
	} else if data[ptr] == nil {
		return nil, ErrObjectNotFound
	}
	return data[ptr], nil
}

func (o *oblivious) startAccess(ctx context.Context, ptrs []uint64) (map[uint64]uint64, error) {
	if len(ptrs) == 0 {
		return nil, nil
	} else if o.store.Count == 0 {
		out := make(map[uint64]uint64)
		for _, ptr := range ptrs {
			out[ptr] = 0
		}
		return out, nil
	}

	// Lookup the leaf each pointer is currently assigned to.
	assignments, err := o.store.Lookup(ctx, ptrs)
	if err != nil {
		return nil, err
	}

	// Assign random leafs to pointers that don't exist.
	maxLeaf := big.NewInt(0).SetUint64(o.store.Count)
	for _, ptr := range ptrs {
		if _, ok := assignments[ptr]; ok {
			continue
		}
		leaf, err := rand.Int(rand.Reader, maxLeaf)
		if err != nil {
			return nil, err
		}
		assignments[ptr] = leaf.Uint64()
	}

	// Compute the buckets/nodes that need to be read.
	maxNode := treeWidth(o.store.Count)
	root := rootNode(o.store.Count)

	nodesDedup := make(map[uint64]struct{})
	for _, leaf := range assignments {
		for node := 2 * leaf; node != root; node = parentStep(node) {
			if node < maxNode {
				nodesDedup[node] = struct{}{}
			}
		}
	}
	nodesDedup[root] = struct{}{}

	// Read all of the requested blocks into the stash.
	nodes := make([]uint64, 0, len(nodesDedup))
	for node, _ := range nodesDedup {
		nodes = append(nodes, node)
	}
	data, err := o.base.GetMany(ctx, nodes)
	if err != nil {
		return nil, err
	}

	stash := make(map[uint64][]byte)
	for id, raw := range data {
		bucket, err := unmarshalBucket(raw, o.maxSize)
		if err != nil {
			return nil, fmt.Errorf("oblivious: failed to parse bucket %v: %v", id, err)
		}
		for ptr, data := range bucket {
			stash[ptr] = data
		}
	}
	for ptr, data := range stash {
		if _, ok := o.store.Stash[ptr]; ok {
			continue
		}
		o.store.Stash[ptr] = data
	}

	return assignments, nil
}

func (o *oblivious) finishAccess(ctx context.Context, assignments map[uint64]uint64) error {
	// Make a note of all the leaf nodes we accessed. This will be used to help
	// us construct the blocks.
	nodes := make(map[uint64]struct{})
	for _, leaf := range assignments {
		nodes[2*leaf] = struct{}{}
	}

	// Assign new leaf nodes to every pointer that was looked up.
	maxLeaf := big.NewInt(0).SetUint64(o.store.Count)
	for ptr, _ := range assignments {
		leaf, err := rand.Int(rand.Reader, maxLeaf)
		if err != nil {
			return err
		}
		assignments[ptr] = leaf.Uint64()
	}
	o.store.Assign(assignments)

	// There's a lot of stuff in the stash unrelated to our query. Lookup the
	// assigned leafs for all those pointers and merge them into `assignments`.
	extraPtrs := make([]uint64, 0)
	for ptr, _ := range o.store.Stash {
		if _, ok := assignments[ptr]; !ok {
			extraPtrs = append(extraPtrs, ptr)
		}
	}
	extra, err := o.store.Lookup(ctx, extraPtrs)
	if err != nil {
		return err
	}
	for ptr, leaf := range extra {
		assignments[ptr] = leaf
	}

	// Convert all assignments from leaf ids to node ids.
	for ptr, leaf := range assignments {
		assignments[ptr] = 2 * leaf
	}

	// Start collecting things in the stash into blocks, starting from the leafs
	// and moving up.
	maxNode := treeWidth(o.store.Count)
	root := rootNode(o.store.Count)

	for {
		for node, _ := range nodes {
			if node >= maxNode {
				continue
			} else if err := o.buildBucket(ctx, node, assignments); err != nil {
				return err
			}
		}

		// Detect if we just built the root bucket and stop working if so.
		if _, ok := nodes[root]; ok {
			break
		}

		// Move everything one level up.
		newNodes := make(map[uint64]struct{})
		for node, _ := range nodes {
			newNodes[parentStep(node)] = struct{}{}
		}
		nodes = newNodes

		for ptr, node := range assignments {
			assignments[ptr] = parentStep(node)
		}
	}

	return nil
}

func (o *oblivious) buildBucket(ctx context.Context, node uint64, assignments map[uint64]uint64) error {
	items := make(map[uint64][]byte)
	itemsRollback := make(map[uint64][]byte)

	// Select at most `blockSize` number of items from the stash assigned to
	// this node.
	for ptr, cand := range assignments {
		if node != cand {
			continue
		}
		val, ok := o.store.Stash[ptr]
		if !ok {
			continue
		}

		items[ptr] = val
		if orig, ok := o.originalVals[ptr]; ok {
			itemsRollback[ptr] = orig
		} else {
			itemsRollback[ptr] = val
		}
		delete(o.store.Stash, ptr)

		if len(items) == blockSize {
			break
		}
	}

	o.rollbackWrites[node] = marshalBucket(itemsRollback, o.maxSize)
	return o.base.Set(ctx, node, marshalBucket(items, o.maxSize), Content)
}

func (o *oblivious) GetMany(ctx context.Context, ptrs []uint64) (map[uint64][]byte, error) {
	if o.needRollback {
		return nil, fmt.Errorf("oblivious: an error condition has occurred, please rollback")
	}
	out := make(map[uint64][]byte)
	if len(ptrs) == 0 || o.store.Count == 0 {
		return out, nil
	}

	assignments, err := o.startAccess(ctx, ptrs)
	if err != nil {
		o.needRollback = true
		return nil, err
	}
	for _, ptr := range ptrs {
		data, ok := o.store.Stash[ptr]
		if ok {
			out[ptr] = dup(data)
		}
	}
	if err := o.finishAccess(ctx, assignments); err != nil {
		o.needRollback = true
		return nil, err
	}

	return out, nil
}

func (o *oblivious) Set(ctx context.Context, ptr uint64, data []byte, _ DataType) error {
	if o.needRollback {
		return fmt.Errorf("oblivious: an error condition has occurred, please rollback")
	}

	assignments, err := o.startAccess(ctx, []uint64{ptr})
	if err != nil {
		o.needRollback = true
		return err
	}

	if o.store.Count <= ptr {
		o.store.Count = ptr + 1
	}
	if _, ok := o.originalVals[ptr]; !ok {
		o.originalVals[ptr] = o.store.Stash[ptr]
	}
	o.store.Stash[ptr] = dup(data)

	if err := o.finishAccess(ctx, assignments); err != nil {
		o.needRollback = true
		return err
	}

	return nil
}

func (o *oblivious) Commit(ctx context.Context) error {
	if o.needRollback {
		return fmt.Errorf("oblivious: an error condition has occurred, please rollback")
	}

	if err := o.store.Commit(ctx, o.integ.curr.Version); err != nil {
		o.base.Rollback(ctx)
		return err
	}
	return o.base.Commit(ctx)
}

func (o *oblivious) Rollback(ctx context.Context) {
	defer func() {
		o.originalVals = nil
		o.rollbackWrites = nil
	}()
	if o.needRollback {
		o.store.Rollback(ctx)
		o.base.Rollback(ctx)
		return
	}

	// When Rollback is called and there haven't been any error conditions, we
	// actually need to commit. This is because Get requests modify the user's
	// data and we need to persist those changes to preserve the privacy
	// guarantees of ORAM. Changes from Set requests are explicitly undone.
	for node, data := range o.rollbackWrites {
		if err := o.base.Set(ctx, node, data, Content); err != nil {
			o.needRollback = true
			o.Rollback(ctx)
			return
		}
	}

	if err := o.store.Commit(ctx, o.integ.curr.Version); err != nil {
		o.base.Rollback(ctx)
		return
	}
	o.base.Commit(ctx)
	return
}

// All the code below this line is only used for testing.

func (o *oblivious) dirtyRollback(ctx context.Context) {
	if err := o.store.Commit(ctx, o.integ.curr.Version); err != nil {
		panic(err)
	}
	o.base.Rollback(ctx)
}

type oramAuditor struct {
	base BlockStorage

	blocksRead, blocksWritten map[uint64]int
}

func unwrapAuditor(base BlockStorage) BlockStorage {
	if auditor, ok := base.(*oramAuditor); ok {
		return auditor.base
	}
	return base
}

func (oa *oramAuditor) Start(ctx context.Context, _ []uint64) (map[uint64][]byte, error) {
	oa.blocksRead, oa.blocksWritten = make(map[uint64]int), make(map[uint64]int)
	return oa.base.Start(ctx, nil)
}

func (oa *oramAuditor) Get(ctx context.Context, ptr uint64) ([]byte, error) {
	oa.blocksRead[ptr] += 1
	return oa.base.Get(ctx, ptr)
}

func (oa *oramAuditor) GetMany(ctx context.Context, ptrs []uint64) (map[uint64][]byte, error) {
	for _, ptr := range ptrs {
		oa.blocksRead[ptr] += 1
	}
	return oa.base.GetMany(ctx, ptrs)
}

func (oa *oramAuditor) Set(ctx context.Context, ptr uint64, data []byte, dt DataType) error {
	oa.blocksWritten[ptr] += 1
	if len(data) != 112 {
		panic("tried to set block of data with wrong size")
	}
	return oa.base.Set(ctx, ptr, data, dt)
}

func (oa *oramAuditor) Commit(ctx context.Context) error { return oa.base.Commit(ctx) }
func (oa *oramAuditor) Rollback(ctx context.Context)     { oa.base.Rollback(ctx) }
