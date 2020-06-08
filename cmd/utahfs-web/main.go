// Command utahfs-web provides access to a UtahFS repository through a Web UI.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/cloudflare/utahfs"
	"github.com/cloudflare/utahfs/cmd/internal/config"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError) // Overwrite the fucking glog flags.
	configPath := flag.String("cfg", "./utahfs.yaml", "Location of the client's config file.")
	serverAddr := flag.String("server-addr", "localhost:3004", "Address to serve data on.")
	metricsAddr := flag.String("metrics-addr", "localhost:3005", "Address to serve metrics on.")
	flag.Parse()

	cfg, err := config.ClientFromFile(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	bfs, err := cfg.FS("./")
	if err != nil {
		log.Fatalf("failed to initialize storage: %v", err)
	}
	fs, err := utahfs.NewArchive(bfs)
	if err != nil {
		log.Fatal(err)
	}

	s := &http.Server{
		Addr:    *serverAddr,
		Handler: http.FileServer(&FileSystem{fs}),
	}

	go metrics(*metricsAddr)
	log.Fatal(s.ListenAndServe())
}
