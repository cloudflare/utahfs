package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/Bren2010/utahfs/persistent"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics registers metrics with Prometheus and starts the server.
func metrics() {
	registry := []prometheus.Collector{
		persistent.AppStorageCommits, persistent.LocalWALSize,
	}

	for i, coll := range registry {
		err := prometheus.Register(coll)
		if err != nil {
			log.Fatalf("%v (metric %v)", err, i)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/" {
			fmt.Fprintln(rw, "Hello, I'm a utahfs-client's metrics and debugging server! Who are you?")
		} else {
			rw.WriteHeader(404)
			fmt.Fprintln(rw, "404 not found")
		}
	})
	mux.Handle("/metrics", prometheus.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := http.Server{
		Addr:    "localhost:" + os.Getenv("METRICS_PORT"),
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}
