package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/cloudflare/utahfs/cmd/internal/config"
)

type TreeSize interface {
	TreeSize() uint64
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	ctx := context.Background()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError) // Overwrite the fucking glog flags.
	configPath := flag.String("cfg", "./utahfs.yaml", "Location of the client's config file.")
	flag.Parse()

	// Read client's config and initialize storage.
	cfg, err := config.ClientFromFile(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	} else if cfg.DataDir == "" {
		log.Fatalf("data-dir must be specified in config")
	}
	block, err := cfg.Block(true)
	if err != nil {
		log.Fatalf("failed to initialize storage: %v", err)
	}

	// Read every block from storage and report integrity errors.
	if _, err = block.Start(ctx, nil); err != nil {
		log.Fatalf("failed to start transaction: %v", err)
	}
	size := block.(TreeSize).TreeSize()
	log.Printf("Total blocks: %v", size)

	for ptr := uint64(0); ptr < size; ptr++ {
		if ptr%10000 == 0 {
			log.Printf("... read %v blocks", ptr)
		}
		if _, err = block.Get(ctx, ptr); err != nil {
			log.Printf("%x: failed to read: %v", ptr, err)
		}
	}

	block.Rollback(ctx)
	log.Println("Done!")
}
