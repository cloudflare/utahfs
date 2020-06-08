package utahfs

import (
	"testing"

	"bytes"
	"context"
	crand "crypto/rand"
	"io"
	"math/rand"
	"time"

	"github.com/cloudflare/utahfs/persistent"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type testData struct {
	bf   *BlockFile
	pos  int
	data []byte
}

func testBFS(t *testing.T, td *testData) {
	bf, pos, data := td.bf, td.pos, td.data

	dice := rand.Intn(30)

	if len(data) > 0 && dice == 0 { // Seek from start.
		off := rand.Int63n(int64(len(data)))
		got, err := bf.Seek(off, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		pos = int(off)
		if got != int64(pos) {
			t.Fatalf("%v != %v", got, pos)
		}
	} else if len(data) > 0 && pos != len(data) && dice == 1 { // Seek from current.
		off := rand.Int63n(int64(len(data) - pos))
		got, err := bf.Seek(off, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		pos += int(off)
		if got != int64(pos) {
			t.Fatalf("%v != %v", got, pos)
		}
	} else if len(data) > 0 && dice == 2 { // Seek from end.
		off := rand.Int63n(int64(len(data)))
		got, err := bf.Seek(-off, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}
		pos = len(data) - int(off)
		if got != int64(pos) {
			t.Fatalf("%v != %v", got, pos)
		}
	} else if len(data) > 0 && dice == 3 { // Truncate.
		size := rand.Int63n(int64(len(data)))
		err := bf.Truncate(size)
		if err != nil {
			t.Fatal(err)
		}
		pos = int(size)
		data = data[:size]
	} else if len(data) > 0 && dice < 20 { // Read.
		p := make([]byte, rand.Int63n(256)+1)

		n, err := bf.Read(p)
		if pos == len(data) {
			if err != io.EOF {
				t.Fatal("expected EOF when reading at end of file")
			} else if n != 0 {
				t.Fatal("read past end of file")
			}
		} else if pos != len(data) {
			if err != nil {
				t.Fatal(err)
			} else if n == 0 {
				t.Fatal("read zero bytes even though there was data")
			}
		}
		if n > len(p) || pos+n > len(data) {
			t.Fatalf("%v violates bounds", n)
		} else if !bytes.Equal(data[pos:pos+n], p[:n]) {
			t.Fatal("read unexpected data")
		}
		pos += n
	} else { // Write.
		p := make([]byte, rand.Int63n(3*256)+1)
		crand.Read(p)

		n, err := bf.Write(p)
		if err != nil {
			t.Fatal(err)
		} else if n != len(p) {
			t.Fatalf("%v != %v", n, len(p))
		}

		if pos+n > len(data) {
			data = append(data[:pos], p...)
		} else {
			copy(data[pos:pos+n], p)
		}
		pos += n
	}

	td.pos, td.data = pos, data
}

func TestBlockFilesystemSplitPtrs(t *testing.T) {
	testBlockFilesystem(t, true)
}

func TestBlockFilesystem(t *testing.T) {
	testBlockFilesystem(t, false)
}

func testBlockFilesystem(t *testing.T, splitPtrs bool) {
	ctx := context.Background()

	store := persistent.NewAppStorage(persistent.NewBlockMemory())
	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}
	bfs, err := NewBlockFilesystem(store, 3, 256, splitPtrs)
	if err != nil {
		t.Fatal(err)
	}

	ptrs := make([]uint64, 0)
	files := make(map[uint64]*testData)

	for i := 0; i < 100000; i++ {
		dice := rand.Intn(10000)

		if len(ptrs) == 0 || dice == 0 {
			ptr, bf, err := bfs.Create(ctx, persistent.Content)
			if err != nil {
				t.Fatal(err)
			}
			ptrs = append(ptrs, ptr)
			files[ptr] = &testData{bf, 0, nil}
		} else {
			ptr := ptrs[rand.Intn(len(ptrs))]
			testBFS(t, files[ptr])
		}
	}

	t.Logf("created %v files", len(ptrs))
	sum := 0
	for _, ptr := range ptrs {
		n := len(files[ptr].data)
		sum += n
		t.Logf("- %x: contains %v bytes", ptr, n)
	}
	t.Logf("%v bytes total", sum)
}
