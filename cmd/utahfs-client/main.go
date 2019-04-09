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

	store, err := storage.NewB2(
		os.Getenv("B2_ACCT_ID"), os.Getenv("B2_APP_KEY"),
		os.Getenv("B2_BUCKET"), os.Getenv("B2_URL"),
	)
	if err != nil {
		log.Fatal(err)
	}
	store, err = storage.NewRetry(store, 3)
	if err != nil {
		log.Fatal(err)
	}
	store, err = storage.NewCache(store, 512)
	if err != nil {
		log.Fatal(err)
	}
	appStore, err := utahfs.NewLocalWAL(store, path.Join(pwd, "utahfs-wal"), 512)
	if err != nil {
		log.Fatal(err)
	}

	bs := utahfs.NewBasicBlockStorage(appStore)
	bfs, err := utahfs.NewBlockFilesystem(bs, 12, 1024*1024)
	if err != nil {
		log.Fatal(err)
	}

	fs, err := utahfs.NewFilesystem(appStore, bfs)
	if err != nil {
		log.Fatal(err)
	}

	host := fuse.NewFileSystemHost(fs)
	host.SetCapReaddirPlus(true)
	host.Mount(path.Join(pwd, "utahfs"), nil)
}
