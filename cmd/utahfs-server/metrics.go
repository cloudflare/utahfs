package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"

	"github.com/cloudflare/utahfs/persistent"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	prometheus.MustRegister(persistent.AppStorageCommits)
	prometheus.MustRegister(persistent.LocalWALSize)
	prometheus.MustRegister(persistent.DiskCacheSize)
	prometheus.MustRegister(persistent.B2Ops)
	prometheus.MustRegister(persistent.GCSOps)
	prometheus.MustRegister(persistent.S3Ops)
}

// metrics registers metrics with Prometheus and starts the server.
func metrics(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/" {
			fmt.Fprintln(rw, "Hello, I'm a utahfs-server's metrics and debugging server! Who are you?")
		} else {
			http.NotFound(rw, req)
		}
	})
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := http.Server{
		Addr:    addr,
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}
