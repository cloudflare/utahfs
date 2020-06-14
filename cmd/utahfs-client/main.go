// Command utahfs-client provides a FUSE binding, backed by an encrypted object
// storage provider.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"

	"github.com/cloudflare/utahfs"
	"github.com/cloudflare/utahfs/cmd/internal/config"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError) // Overwrite the fucking glog flags.
	configPath := flag.String("cfg", "./utahfs.yaml", "Location of the client's config file.")
	mountPath := flag.String("mount", "./utahfs", "Directory to mount as remote drive.")
	verbose := flag.Bool("v", false, "Enable debug logging.")
	metricsAddr := flag.String("metrics-addr", "localhost:3001", "Address to serve metrics on.")
	flag.Parse()

	fullMountPath, err := filepath.Abs(*mountPath)
	if err != nil {
		log.Fatalf("failed to resolve mount path: %v", err)
	}
	volume := path.Base(fullMountPath)

	cfg, err := config.ClientFromFile(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	bfs, err := cfg.FS(fullMountPath)
	if err != nil {
		log.Fatalf("failed to initialize storage: %v", err)
	}

	var fs fuseutil.FileSystem
	if cfg.Archive {
		fs, err = utahfs.NewArchive(bfs)
	} else {
		fs, err = utahfs.NewFilesystem(bfs)
	}
	if err != nil {
		log.Fatal(err)
	}
	server := fuseutil.NewFileSystemServer(fs)

	mountCfg := &fuse.MountConfig{
		FSName:      volume,
		ErrorLogger: log.New(os.Stderr, "fuse: ", log.Flags()),
		VolumeName:  volume,
		Subtype:     "utahfs",
	}
	if *verbose {
		mountCfg.DebugLogger = log.New(os.Stderr, "fuse-debug: ", log.Flags())
	}
	mfs, err := fuse.Mount(fullMountPath, server, mountCfg)
	if err != nil {
		log.Fatal(err)
	}
	go handleInterrupt(mfs.Dir())
	go metrics(*metricsAddr)

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
