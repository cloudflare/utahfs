package persistent

import (
	"context"
	"crypto/rand"
	"fmt"
)

const (
	// blockSize is the number of entries that go in a single block.
	blockSize int = 4
)

type oblivious struct {
	base  BlockStorage
	store *obliviousStore

	integ *integrity

	needsCommit bool
}

// WithORAM wraps a BlockStorage implementation and prevents outsiders from
// seeing the user's access pattern. This includes which data a user is
// accessing and whether the user is reading or writing, but not the total
// amount of accesses or when.
//
// A small amount of local data is stored in `loc`.
func WithORAM(base BlockStorage, store ObliviousStorage) (BlockStorage, error) {
	enc, ok := base.(*encryption)
	if !ok {
		return nil, fmt.Errorf("oblivious: expected encryption layer as input, but got: %T", base)
	}
	integ, ok := enc.base.(*integrity)
	if !ok {
		return nil, fmt.Errorf("oblivious: expected integrity layer below encryption layer, but got: %T", enc.base)
	}

	return &oblivious{
		base:  base,
		store: newObliviousStore(store),

		integ: integ,
	}, nil
}

func (o *oblivious) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	if len(prefetch) > 0 {
		return nil, fmt.Errorf("oblivious: prefetch is not supported")
	} else if _, err := o.base.Start(ctx, nil); err != nil {
		return nil, err
	} else if err := o.store.Start(ctx); err != nil {
		o.base.Rollback(ctx)
		return nil, err
	}
	return nil, err
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
	// Lookup the leaf each pointer is currently assigned to.
	assignments, err := o.store.Lookup(ctx, ptrs)
	if err != nil {
		return nil, err
	}

	// Compute the buckets/nodes that need to be read.
	maxNode := treeWidth(o.store.Count)
	root := rootNode(o.store.Count)

	nodesDedup := make(map[uint64]struct{})
	for ptr, leaf := range assignments {
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
		bucket, err := unmarshalBucket(raw)
		if err != nil {
			return nil, fmt.Errorf("oblivious: failed to parse bucket %v: %v", id, err)
		}
		for ptr, data := range bucket {
			stash[ptr] = data
		}
	}
	for ptr, data := range stash {
		o.store.Stash[ptr] = data
	}

	return assigments, nil
}

func (o *oblivious) finishAccess(ctx context.Context, assignments map[uint64]uint64) error {
	maxLeaf := big.NewInt(0).SetUint64(o.store.Count)

	newLeafs := make(map[uint64]uint64)
	for ptr, _ := range currLeafs {
		// Note that we're only re-assigning the pointers we successfully looked
		// up. Pointers without a mapping to a leaf are non-existent.
		n, err := rand.Int(rand.Reader, maxLeaf)
		if err != nil {
			return nil, err
		}
		newLeafs[ptr] = n.Uint64()
	}

	// Make a note of all the leaf nodes we accessed. This will be used to help
	// us construct the blocks.
	nodes := make(map[uint64]struct{})
	for _, leaf := range assignments {
		nodes[2*leaf] = struct{}{}
	}

	// There's a lot of stuff in the stash unrelated to our query. Lookup the
	// assigned leafs for all those pointers and merge it into `assignments`.
	extraPtrs := make([]uint64, 0)
	for ptr, _ := range o.store.Stash {
		if _, ok := assignments[ptr]; !ok {
			extraPtrs = append(extraPtrs, ptr)
		}
	}
	extra, err := o.store.Lookup(ctx, extraPtrs)
	if err != nil {
		return nil, err
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
			}

			// Select `blockSize` number of items from the stash assigned to
			// this node.
			items := make(map[uint64][]byte)
			for ptr, cand := range assignments {
				if node == cand {
					items[ptr] = o.store.Stash[ptr]
					delete(o.store.Stash, ptr)
					if len(items) == blockSize {
						break
					}
				}
			}
			if err := o.base.Set(ctx, node, marshalBucket(items)); err != nil {
				return nil, err
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

func (o *oblivious) GetMany(ctx context.Context, ptrs []uint64) (map[uint64][]byte, error) {
	out := make(map[uint64][]byte)
	if len(ptrs) == 0 {
		return out, nil
	}

	assignments, err := o.startAccess(ctx, ptrs)
	if err != nil {
		return nil, err
	}
	for _, ptr := range ptrs {
		data, ok := o.store.Stash[ptr]
		if ok {
			out[ptr] = dup(data)
		}
	}
	if err := o.finishAccess(ctx, assignments); err != nil {
		return nil, err
	}

	return out, nil
}

// func (o *oblivious) Set(ctx context.Context, ptr uint64, data []byte, dt DataType) error {
// }
//
// func (o *oblivious) Commit(ctx context.Context) error { return o.base.Commit(ctx) }
// func (o *oblivious) Rollback(ctx context.Context)     { o.base.Rollback(ctx) }
