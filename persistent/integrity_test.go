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

func TestIntegrity(t *testing.T) {
	ctx := context.Background()

	// Choose a temp directory for the pin file, and setup the storage.
	name, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(name)

	store := NewBlockMemory()
	temp, err := WithIntegrity(store, "password", name+"/pin.json")
	if err != nil {
		t.Fatal(err)
	}
	appStore := NewAppStorage(temp)

	// Make a bunch of random writes. Check that random reads succeed as well.
	writtenPtrs := make([]uint64, 0)
	written := make(map[uint64][]byte)

	if err := appStore.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		// Random write.
		ptr := uint64(mrand.Intn(600))
		data := make([]byte, 64)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		writtenPtrs = append(writtenPtrs, ptr)
		written[ptr] = dup(data)

		if err := appStore.Set(ctx, ptr, data, Content); err != nil {
			t.Fatal(err)
		}

		// Random read.
		ptr = writtenPtrs[mrand.Intn(len(writtenPtrs))]
		data, err = appStore.Get(ctx, ptr)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(data, written[ptr]) {
			t.Fatal("data not equal to expected")
		}
	}
	if err := appStore.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
