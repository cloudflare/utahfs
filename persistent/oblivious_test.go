package persistent

import (
	"testing"

	"bytes"
	"context"
	"crypto/rand"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"time"
)

func init() {
	mrand.Seed(time.Now().UnixNano())
}

func TestOblivious(t *testing.T) {
	// Choose a temp directory for the databases and pin file.
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Setup client-side ORAM storage.
	localStore, err := NewLocalOblivious(tempDir + "/oram")
	if err != nil {
		t.Fatal(err)
	}

	// Setup block storage.
	disk, err := NewDisk(tempDir + "/db")
	if err != nil {
		t.Fatal(err)
	}
	base := NewBufferedStorage(NewSimpleReliable(disk))

	integ, err := WithIntegrity(base, "password", tempDir+"/pin.json")
	if err != nil {
		t.Fatal(err)
	}
	enc := WithEncryption(integ, "password")
	auditor := &oramAuditor{base: enc}

	store, err := WithORAM(auditor, localStore, 16)
	if err != nil {
		t.Fatal(err)
	}

	// Run tests.
	t.Run("Correctness", testORAMCorrectness(store))
	t.Run("Randomness", testORAMRandomness(auditor, store))
}

// testORAMCorrectness checks that data stored in ORAM can be successfully
// fetched later, provided that it hasn't been rolled back.
func testORAMCorrectness(store BlockStorage) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		backup := NewBlockMemory()

		for i := 0; i < 50; i++ {
			// Start a transaction.
			if _, err := store.Start(ctx, nil); err != nil {
				t.Fatal(err)
			}
			rollback := mrand.Intn(3) == 2
			dirty := mrand.Intn(3) == 2

			// Make a series of random writes to store that may or may not be
			// rolled back.
			for i := 0; i < 10; i++ {
				ptr := uint64(mrand.Intn(100))
				val := make([]byte, mrand.Intn(15)+1)
				if _, err := rand.Read(val); err != nil {
					t.Fatal(err)
				}

				if err := store.Set(ctx, ptr, dup(val), Content); err != nil {
					t.Fatal(err)
				}
				if !rollback {
					backup.Set(ctx, ptr, val, Content)
				}
			}

			// Rollback if needed.
			if rollback {
				if dirty {
					store.(*oblivious).dirtyRollback(ctx)
				} else {
					store.Rollback(ctx)
				}
				continue
			}

			// Do a series of random reads and check for consistency.
			for i := 0; i < 10; i++ {
				ptr := uint64(mrand.Intn(100))

				val1, err1 := store.Get(ctx, ptr)
				val2, err2 := backup.Get(ctx, ptr)

				if err1 == ErrObjectNotFound && err2 == ErrObjectNotFound {
					continue
				} else if err1 != nil {
					t.Fatal(err1)
				} else if !bytes.Equal(val1, val2) {
					t.Fatal("ORAM storage contains a different value than backup!")
				}
			}

			// Commit changes.
			if err := store.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func testORAMRandomness(auditor *oramAuditor, store BlockStorage) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		// Ensure the number of blocks is a power of two.
		if _, err := store.Start(ctx, nil); err != nil {
			t.Fatal(err)
		}
		for ptr := uint64(0); ptr < 256; ptr++ {
			if err := store.Set(ctx, ptr, make([]byte, 16), Content); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		// Make the same read several times and keep track of how often each
		// node is accessed.
		const n = 100
		totalAccesses := make(map[uint64]int)

		for i := 0; i < n; i++ {
			if _, err := store.Start(ctx, nil); err != nil {
				t.Fatal(err)
			} else if _, err := store.Get(ctx, 0); err != nil {
				t.Fatal(err)
			} else if err := store.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			for node, count := range auditor.blocksRead {
				totalAccesses[node] += count
			}
		}

		// Check that the number of times each node was accessed is proportional
		// to its level in the tree.
		levels := make(map[uint64][]int)
		for i := uint64(0); i < 9; i++ {
			levels[i] = make([]int, 1<<(8-i))
		}
		for node := uint64(0); node < treeWidth(256); node++ {
			l := level(node)

			offset := (uint64(1) << l) - 1
			scale := 2*offset + 2

			levelId := (node - offset) / scale

			levels[l][levelId] += totalAccesses[node]
		}

		for i := uint64(0); i < 9; i++ {
			t.Logf("Level %v: %v", i, levels[i])

			if i > 4 {
				for _, count := range levels[i] {
					if count == 0 {
						t.Fatal("ORAM accesses aren't sufficiently distributed across the tree")
					}
				}
			}
		}
	}
}
