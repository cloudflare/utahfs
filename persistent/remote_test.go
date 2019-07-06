package persistent

import (
	"testing"

	"net/http"
	"sync"
	"time"
)

func TestRemoteConnections(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()

	go func() {
		cfg, err := generateConfig("myPassword", "utahfs-test-server")
		if err != nil {
			t.Fatal(err)
		}
		s := http.Server{
			Addr: "localhost:62849",
			Handler: http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				if len(req.TLS.VerifiedChains) > 0 {
					rw.WriteHeader(200)
				} else {
					rw.WriteHeader(500)
				}
			}),

			TLSConfig: cfg,
		}
		mu.Unlock()
		t.Fatal(s.ListenAndServeTLS("", ""))
	}()

	mu.Lock()
	time.Sleep(100 * time.Millisecond)

	cfg, err := generateConfig("myPassword", "utahfs-test-client")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ServerName = "utahfs-test-server"

	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: cfg},
		Timeout:   30 * time.Second,
	}
	resp, err := client.Get("https://localhost:62849")
	if err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != 200 {
		t.Fatalf("unexpected response status: %v", resp.Status)
	}
}
