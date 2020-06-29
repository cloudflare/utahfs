package persistent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"

	"golang.org/x/crypto/argon2"
)

func generateConfig(transportKey, hostname string) (*tls.Config, error) {
	curve := elliptic.P256()

	// NOTE: The fixed salt to Argon2 is intentional. Its purpose is domain
	// separation, not to frustrate a password cracker.

	// Generate root CA certificate.
	key := argon2.IDKey([]byte(transportKey), []byte("da61d4a0469fdb7f"), 1, 64*1024, 4, 32)
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

func writeMap(w io.Writer, data map[uint64][]byte) error {
	for key, val := range data {
		hdr := make([]byte, 2*binary.MaxVarintLen64)

		n := binary.PutUvarint(hdr, key)
		m := binary.PutUvarint(hdr[n:], uint64(len(val)))

		if _, err := w.Write(hdr[:n+m]); err != nil {
			return err
		} else if _, err := w.Write(val); err != nil {
			return err
		}
	}
	return nil
}

func readMap(r io.Reader) (map[uint64][]byte, error) {
	br := bufio.NewReader(r)
	out := make(map[uint64][]byte)

	for {
		key, err := binary.ReadUvarint(br)
		if err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, err
		}

		valLen, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(br, val); err != nil {
			return nil, err
		}
		out[key] = val
	}
}

func parseKeys(in []string) ([]uint64, error) {
	out := make([]uint64, 0, len(in))

	for _, keyStr := range in {
		key, err := strconv.ParseUint(keyStr, 16, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, key)
	}

	return out, nil
}

type remoteClient struct {
	mu sync.Mutex

	serverUrl *url.URL
	client    *http.Client
	oram      bool

	id string
}

// NewRemoteClient returns a ReliableStorage implementation that defers reads
// and writes to a remote server.
//
// The corresponding server implementation is in NewRemoteServer.
func NewRemoteClient(transportKey, serverUrl string, oram bool) (ReliableStorage, error) {
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
	cfg.ServerName = "utahfs-server"
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

			TLSClientConfig:    cfg,
			DisableCompression: true,
		},

		Timeout: 30 * time.Second,
	}

	rc := &remoteClient{
		serverUrl: parsed,
		client:    client,
		oram:      oram,
	}
	go rc.maintain()
	return rc, nil
}

func (rc *remoteClient) get(ctx context.Context, loc string) (map[uint64][]byte, error) {
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
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("remote: unexpected response status: %v: %v", loc, resp.Status)
	}
	return readMap(resp.Body)
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
		resp.Body.Close()
		return fmt.Errorf("remote: unexpected response status: %v: %v", loc, resp.Status)
	}
	resp.Body.Close()
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
			// Sometimes we'll ping a transaction that was closed after we got
			// the current id but before the server saw our ping request. It's
			// easiest to just ignore these errors.
			if strings.HasSuffix(err.Error(), "401 Unauthorized") {
				continue
			}
			log.Println(err)
		}
	}
}

func (rc *remoteClient) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	// Generate a random transaction id, let the server know about it, and store
	// it in `rc` so that the maintainer thread knows about it.
	if rc.getId() != "" {
		return nil, fmt.Errorf("remote: transaction already started")
	}
	buff := make([]byte, 12)
	if _, err := rand.Read(buff); err != nil {
		return nil, err
	}
	id := fmt.Sprintf("%x", buff)

	loc := "start?id=" + id
	for _, key := range prefetch {
		loc += "&key=" + hex(key)
	}
	if rc.oram {
		loc += "&oram=true"
	}
	data, err := rc.get(ctx, loc)
	if err != nil {
		return nil, err
	}

	rc.mu.Lock()
	if rc.id != "" {
		return nil, fmt.Errorf("remote: transaction already started")
	}
	rc.id = id
	rc.mu.Unlock()
	return data, nil
}

func (rc *remoteClient) Get(ctx context.Context, key uint64) ([]byte, error) {
	data, err := rc.GetMany(ctx, []uint64{key})
	if err != nil {
		return nil, err
	} else if data[key] == nil {
		return nil, ErrObjectNotFound
	}
	return data[key], nil
}

func (rc *remoteClient) GetMany(ctx context.Context, keys []uint64) (map[uint64][]byte, error) {
	id := rc.getId()
	if id == "" {
		return nil, fmt.Errorf("remote: transaction not active")
	}
	loc := "get?id=" + id
	for _, key := range keys {
		loc += "&key=" + hex(key)
	}
	return rc.get(ctx, loc)
}

func (rc *remoteClient) Commit(ctx context.Context, writes map[uint64]WriteData) error {
	id := rc.getId()
	if id == "" {
		return fmt.Errorf("remote: transaction not active")
	}
	data := make(map[uint64][]byte)
	for key, wr := range writes {
		if wr.Type < 0 || wr.Type > 255 {
			return fmt.Errorf("remote: write type is out of bounds")
		}
		data[key] = append([]byte{byte(wr.Type)}, wr.Data...)
	}
	buff := &bytes.Buffer{}
	if err := writeMap(buff, data); err != nil {
		return err
	}
	err := rc.post(ctx, "commit?id="+id, buff)

	rc.mu.Lock()
	rc.id = ""
	rc.mu.Unlock()
	return err
}

type remoteServer struct {
	requestMu     sync.Mutex
	transactionMu sync.Mutex
	transactionId string
	lastCheckIn   time.Time

	base ReliableStorage
	oram bool
}

// NewRemoteServer wraps a ReliableStorage implementation in an HTTP handler,
// allowing remote clients to make requests to it.
//
// The corresponding client implementation is in NewRemoteClient.
func NewRemoteServer(base ReliableStorage, transportKey string, oram bool) (*http.Server, error) {
	cfg, err := generateConfig(transportKey, "utahfs-server")
	if err != nil {
		return nil, err
	}
	rs := &remoteServer{base: base, oram: oram}
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
	case req.Method == "GET" && strings.HasSuffix(req.URL.Path, "/start"):
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

	// In case we were hanging for a long time on the lock, quickly check if the
	// client is still here.
	select {
	case <-req.Context().Done():
		rs.transactionMu.Unlock()
		return
	default:
	}

	// Ensure that server and client agree on the use of ORAM.
	clientORAM := req.Form.Get("oram") == "true"
	if rs.oram != clientORAM {
		rs.transactionMu.Unlock()
		log.Println("client and server disagree on whether oram is enabled")
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Start a new transaction, and record initial information about it.
	prefetch, err := parseKeys(req.Form["key"])
	if err != nil {
		rs.transactionMu.Unlock()
		log.Println(err)
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	data, err := rs.base.Start(req.Context(), prefetch)
	if err != nil {
		rs.transactionMu.Unlock()
		log.Println(err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rs.transactionId = req.Form.Get("id")
	rs.lastCheckIn = time.Now()

	rw.WriteHeader(http.StatusOK)
	if err := writeMap(rw, data); err != nil {
		log.Println(err)
		return
	}
}

func (rs *remoteServer) handleGet(rw http.ResponseWriter, req *http.Request) {
	if req.Form.Get("id") != rs.transactionId {
		rw.WriteHeader(http.StatusUnauthorized)
		return
	}

	keys, err := parseKeys(req.Form["key"])
	if err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	data, err := rs.base.GetMany(req.Context(), keys)
	if err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	if err := writeMap(rw, data); err != nil {
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

	data, err := readMap(req.Body)
	if err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	writes := make(map[uint64]WriteData)
	for key, val := range data {
		writes[key] = WriteData{val[1:], DataType(val[0])}
	}
	if err := rs.base.Commit(req.Context(), writes); err != nil {
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
