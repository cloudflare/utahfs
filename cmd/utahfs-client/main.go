package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path"

	"github.com/Bren2010/utahfs"
	"github.com/Bren2010/utahfs/storage"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	volume := path.Base(flag.Arg(0))
	if volume == "." || volume == "/" {
		log.Fatalf("failed to parse mount path")
	}

	store, err := storage.NewS3(
		os.Getenv("S3_APP_ID"), os.Getenv("S3_APP_KEY"),
		os.Getenv("S3_BUCKET"), os.Getenv("S3_URL"), os.Getenv("S3_REGION"),
	)
	if err != nil {
		log.Fatal(err)
	}
	store, err = storage.NewRetry(store, 3)
	if err != nil {
		log.Fatal(err)
	}
	store, err = storage.NewCache(store, 32*1024)
	if err != nil {
		log.Fatal(err)
	}

	walPath := path.Join(path.Dir(flag.Arg(0)), "utahfs-wal")
	relStore, err := utahfs.NewLocalWAL(store, walPath, 32*512)
	if err != nil {
		log.Fatal(err)
	}

	appStore := utahfs.NewAppStorage(relStore)

	bs := utahfs.NewBasicBlockStorage(appStore)
	bfs, err := utahfs.NewBlockFilesystem(bs, 12, 32*1024)
	if err != nil {
		log.Fatal(err)
	}

	fs, err := utahfs.NewFilesystem(appStore, bfs)
	if err != nil {
		log.Fatal(err)
	}
	server := fuseutil.NewFileSystemServer(fs)

	cfg := &fuse.MountConfig{
		FSName: volume,

		ErrorLogger: log.New(os.Stderr, "fuse: ", log.Flags()),
		DebugLogger: log.New(os.Stderr, "fuse-debug: ", log.Flags()),

		VolumeName: volume,
	}
	mfs, err := fuse.Mount(flag.Arg(0), server, cfg)
	if err != nil {
		log.Fatal(err)
	}
	go handleInterrupt(mfs.Dir())

	log.Println("filesystem successfully mounted")
	if err := mfs.Join(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func handleInterrupt(mountPoint string) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	for {
		<-signalChan
		log.Println("Received SIGINT, attempting to unmount...")

		err := fuse.Unmount(mountPoint)
		if err != nil {
			log.Printf("Failed to unmount in response to SIGINT: %v", err)
		} else {
			log.Printf("Successfully unmounted in response to SIGINT.")
			return
		}
	}
}
