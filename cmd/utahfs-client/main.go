package main

import (
	"log"
	"os"
	"path"

	"github.com/Bren2010/utahfs"
	"github.com/Bren2010/utahfs/storage"

	"github.com/billziss-gh/cgofuse/fuse"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	// b2, err := storage.NewB2(
	// 	os.Getenv("B2_ACCT_ID"), os.Getenv("B2_APP_KEY"),
	// 	os.Getenv("B2_BUCKET"), os.Getenv("B2_URL"),
	// )
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// retryB2, err := storage.NewRetry(b2, 3)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	store := utahfs.NewLocalWAL(path.Join(pwd, "utahfs.wal"), storage.NewMemory())

	bs := utahfs.NewBasicBlockStorage(store)
	bfs, err := utahfs.NewBlockFilesystem(bs, 12, 1024*1024)
	if err != nil {
		log.Fatal(err)
	}

	fs, err := utahfs.NewFilesystem(store, bfs)
	if err != nil {
		log.Fatal(err)
	}

	host := fuse.NewFileSystemHost(fs)
	host.SetCapReaddirPlus(true)
	host.Mount(path.Join(pwd, "utahfs"), nil)
}
