// Command utahfs-server acts as a cache to improve the performance of a UtahFS
// instance, and helps coordinate multiple users.
//
// It is meant to be deployed on the same LAN as the user, on semi-trusted
// hardware.
package main

import (
	"flag"
	"log"
	"os"

	"github.com/cloudflare/utahfs/cmd/internal/config"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError) // Overwrite the fucking glog flags.
	configPath := flag.String("cfg", "./utahfs.yaml", "Location of the server's config file.")
	serverAddr := flag.String("server-addr", "0.0.0.0:3002", "Address to expose server on.")
	metricsAddr := flag.String("metrics-addr", "localhost:3003", "Address to serve metrics on.")
	flag.Parse()

	cfg, err := config.ServerFromFile(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	server, err := cfg.Server()
	if err != nil {
		log.Fatalf("failed to initialize server: %v", err)
	}
	server.Addr = *serverAddr

	log.Println("server successfully started")
	go metrics(*metricsAddr)
	log.Fatal(server.ListenAndServeTLS("", ""))
}
