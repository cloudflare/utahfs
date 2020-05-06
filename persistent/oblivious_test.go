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
	base := NewSimpleBlock(NewBufferedStorage(NewSimpleReliable(disk)))

	integ, err := WithIntegrity(base, "password", tempDir+"/pin.json")
	if err != nil {
		t.Fatal(err)
	}
	enc := WithEncryption(integ, "password")

	store, err := WithORAM(enc, localStore, 16)
	if err != nil {
		t.Fatal(err)
	}

	// Run tests.
	t.Run("Correctness", testORAMCorrectness(store))
}

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
			if rollback { // TODO: Also test partial rollback.
				store.Rollback(ctx)
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
