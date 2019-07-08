package persistent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"

	"golang.org/x/crypto/pbkdf2"
)

func generateConfig(transportKey, hostname string) (*tls.Config, error) {
	curve := elliptic.P256()

	// Generate root CA certificate.
	key := pbkdf2.Key([]byte(transportKey), []byte("da61d4a0469fdb7f"), 4096, 32, sha1.New)
	caD := new(big.Int).SetBytes(key)
	caD.Mod(caD, curve.Params().N)
	caPriv := &ecdsa.PrivateKey{D: caD}
	caPriv.PublicKey.Curve = curve
	caPriv.X, caPriv.Y = curve.ScalarBaseMult(caD.Bytes())

	caTempl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "utahfs-ca"},
		NotBefore:    time.Now().Add(-1 * 24 * time.Hour),
		NotAfter:     time.Now().Add(364 * 24 * time.Hour),

		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},

		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caRaw, err := x509.CreateCertificate(rand.Reader, caTempl, caTempl, caPriv.Public(), caPriv)
	if err != nil {
		return nil, err
	}
	caCert, err := x509.ParseCertificate(caRaw)
	if err != nil {
		return nil, err
	}

	// Generate leaf certificate with requested hostname.
	priv, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		return nil, err
	}
	serialLimit := big.NewInt(1)
	serialLimit.Lsh(serialLimit, 64)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, err
	}

	templ := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: hostname},
		NotBefore:    time.Now().Add(-1 * 24 * time.Hour),
		NotAfter:     time.Now().Add(364 * 24 * time.Hour),

		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},

		BasicConstraintsValid: true,
	}
	raw, err := x509.CreateCertificate(rand.Reader, templ, caCert, priv.Public(), caPriv)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(raw)
	if err != nil {
		return nil, err
	}

	// Setup TLS config.
	rootPool := x509.NewCertPool()
	rootPool.AddCert(caCert)

	cfg := &tls.Config{
		Certificates: []tls.Certificate{tls.Certificate{
			Certificate: [][]byte{raw},
			PrivateKey:  priv,
			Leaf:        cert,
		}},

		RootCAs:   rootPool,
		ClientCAs: rootPool,

		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	return cfg, nil
}

type remoteClient struct {
	mu sync.Mutex

	serverUrl *url.URL
	client    *http.Client

	id string
}

// NewRemoteClient returns a ReliableStorage implementation that defers reads
// and writes to a remote server.
//
// The corresponding server implementation is in NewRemoteServer.
func NewRemoteClient(transportKey, serverUrl string) (ReliableStorage, error) {
	parsed, err := url.Parse(serverUrl)
	if err != nil {
		return nil, err
	} else if parsed.Scheme != "https" {
		return nil, fmt.Errorf("remote: server url must start with https://")
	} else if !strings.HasSuffix(parsed.Path, "/") {
		return nil, fmt.Errorf("remote: server url must end with / (forward slash)")
	}

	cfg, err := generateConfig(transportKey, "utahfs-client")
	if err != nil {
		return nil, err
	}
	// Code below is copied from net/http and slightly modified.
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          3,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,

			TLSClientConfig: cfg,
		},

		Timeout: 30 * time.Second,
	}

	rc := &remoteClient{
		serverUrl: parsed,
		client:    client,
	}
	go rc.maintain()
	return rc, nil
}

func (rc *remoteClient) get(ctx context.Context, loc string) ([]byte, error) {
	parsed, err := url.Parse(loc)
	if err != nil {
		return nil, err
	}
	rc.mu.Lock()
	fullLoc := rc.serverUrl.ResolveReference(parsed).String()
	rc.mu.Unlock()

	req, err := http.NewRequest("GET", fullLoc, nil)
	if err != nil {
		return nil, err
	}
	resp, err := rc.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	} else if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote: unexpected response status: %v: %v", loc, resp.Status)
	}
	return ioutil.ReadAll(resp.Body)
}

func (rc *remoteClient) post(ctx context.Context, loc string, body io.Reader) error {
	parsed, err := url.Parse(loc)
	if err != nil {
		return err
	}
	rc.mu.Lock()
	fullLoc := rc.serverUrl.ResolveReference(parsed).String()
	rc.mu.Unlock()

	req, err := http.NewRequest("POST", fullLoc, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := rc.client.Do(req.WithContext(ctx))
	if err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("remote: unexpected response status: %v: %v", loc, resp.Status)
	}
	return nil
}

func (rc *remoteClient) getId() string {
	rc.mu.Lock()
	id := rc.id
	rc.mu.Unlock()
	return id
}

// maintain pings the remote server every 3s if there's an open transaction, to
// let the server know that we're still alive.
func (rc *remoteClient) maintain() {
	ctx := context.Background()

	for {
		time.Sleep(3 * time.Second)

		id := rc.getId()
		if id == "" {
			continue
		}

		if err := rc.post(ctx, "ping?id="+id, nil); err != nil {
			log.Println(err)
		}
	}
}

func (rc *remoteClient) Start(ctx context.Context) error {
	// Generate a random transaction id, let the server know about it, and store
	// it in `rc` so that the maintainer thread knows about it.
	if rc.getId() != "" {
		return fmt.Errorf("remote: transaction already started")
	}
	buff := make([]byte, 12)
	if _, err := rand.Read(buff); err != nil {
		return err
	}
	id := fmt.Sprintf("%x", buff)

	if err := rc.post(ctx, "start?id="+id, nil); err != nil {
		return err
	}

	rc.mu.Lock()
	if rc.id != "" {
		return fmt.Errorf("remote: transaction already started")
	}
	rc.id = id
	rc.mu.Unlock()
	return nil
}

func (rc *remoteClient) Get(ctx context.Context, key string) ([]byte, error) {
	id := rc.getId()
	if id == "" {
		return nil, fmt.Errorf("remote: transaction not active")
	}
	return rc.get(ctx, "get?id="+id+"&key="+url.QueryEscape(key))
}

func (rc *remoteClient) Commit(ctx context.Context, writes map[string][]byte) error {
	id := rc.getId()
	if id == "" {
		return fmt.Errorf("remote: transaction not active")
	}
	buff := &bytes.Buffer{}
	if err := json.NewEncoder(buff).Encode(writes); err != nil {
		return err
	}
	return rc.post(ctx, "commit?id="+id, buff)
}

type remoteServer struct {
	requestMu     sync.Mutex
	transactionMu sync.Mutex
	transactionId string
	lastCheckIn   time.Time

	base ReliableStorage
}

// NewRemoteServer wraps a ReliableStorage implementation in an HTTP handler,
// allowing remote clients to make requests to it.
//
// The corresponding client implementation is in NewRemoteClient.
func NewRemoteServer(base ReliableStorage, transportKey string) (*http.Server, error) {
	cfg, err := generateConfig(transportKey, "utahfs-server")
	if err != nil {
		return nil, err
	}
	rs := &remoteServer{base: base}
	go rs.maintain()

	return &http.Server{
		Handler:   rs,
		TLSConfig: cfg,
	}, nil
}

// maintain cancels transactions that have gone too long since the client last
// checked in.
func (rs *remoteServer) maintain() {
	ctx := context.Background()

	for {
		time.Sleep(1 * time.Second)

		rs.requestMu.Lock()
		if rs.transactionId != "" && time.Since(rs.lastCheckIn) > 5*time.Second {
			rs.transactionMu.Unlock()
			rs.transactionId = ""
			rs.lastCheckIn = time.Time{}

			if err := rs.base.Commit(ctx, nil); err != nil {
				log.Println(err)
			}
		}
		rs.requestMu.Unlock()
	}
}

func (rs *remoteServer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	query, _ := url.ParseQuery(req.URL.RawQuery)
	if query.Get("id") == "" {
		log.Println("remote: client provided no transaction id")
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	req.Form = query

	rs.requestMu.Lock()
	defer rs.requestMu.Unlock()

	switch {
	case req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/start"):
		rs.handleStart(rw, req)
	case req.Method == "GET" && strings.HasSuffix(req.URL.Path, "/get"):
		rs.handleGet(rw, req)
	case req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/commit"):
		rs.handleCommit(rw, req)
	case req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/ping"):
		rs.handlePing(rw, req)
	default:
		http.NotFound(rw, req)
	}
}

func (rs *remoteServer) handleStart(rw http.ResponseWriter, req *http.Request) {
	rs.requestMu.Unlock()

	rs.transactionMu.Lock()

	rs.requestMu.Lock()
	defer rs.requestMu.Unlock()

	// In case we were hanging for a long time on the lock, quickly check if the
	// client is still here.
	select {
	case <-req.Context().Done():
		rs.transactionMu.Unlock()
		return
	default:
	}

	// Start a new transaction, and a goroutine that will stop holding everybody
	// else up if this client disappears.
	if err := rs.base.Start(req.Context()); err != nil {
		rs.transactionMu.Unlock()
		log.Println(err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rs.transactionId = req.Form.Get("id")

	rw.WriteHeader(http.StatusOK)
}

func (rs *remoteServer) handleGet(rw http.ResponseWriter, req *http.Request) {
	if req.Form.Get("id") != rs.transactionId {
		rw.WriteHeader(http.StatusUnauthorized)
		return
	}

	data, err := rs.base.Get(req.Context(), req.Form.Get("key"))
	if err == ErrObjectNotFound {
		rw.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	if _, err := rw.Write(data); err != nil {
		log.Println(err)
		return
	}
}

func (rs *remoteServer) handleCommit(rw http.ResponseWriter, req *http.Request) {
	if req.Form.Get("id") != rs.transactionId {
		rw.WriteHeader(http.StatusUnauthorized)
		return
	}

	defer func() {
		rs.transactionMu.Unlock()
		rs.transactionId = ""
		rs.lastCheckIn = time.Time{}
	}()

	writes := make(map[string][]byte)
	if err := json.NewDecoder(req.Body).Decode(&writes); err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusBadRequest)
		return
	} else if err := rs.base.Commit(req.Context(), writes); err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
}

func (rs *remoteServer) handlePing(rw http.ResponseWriter, req *http.Request) {
	if req.Form.Get("id") != rs.transactionId {
		rw.WriteHeader(http.StatusUnauthorized)
		return
	}
	rs.lastCheckIn = time.Now()
}
