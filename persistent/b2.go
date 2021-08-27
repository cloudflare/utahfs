package persistent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/kothar/go-backblaze.v0"
)

var (
	B2Ops = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "b2_ops",
			Help: "The number of operations against a B2 backend.",
		},
		[]string{"operation", "success"},
	)

	client = &http.Client{
		Transport: &http.Transport{ // copied from net/http.DefaultTransport
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
		},
		Timeout: 30 * time.Second,
	}
)

type b2 struct {
	pool *sync.Pool
	url  string
}

// NewB2 returns object storage backed by Backblaze B2. `acctId` and `appKey`
// are the Account ID and Application Key of a B2 bucket. `bucketName` is the
// name of the bucket. Keys other than the master key can be used by omitting
// the account key and providing the key ID provided by B2 with the key. `url` is
// the URL to use to download data.
func NewB2(acctId, keyId, appKey, bucketName, url string) (ObjectStorage, error) {
	creds := backblaze.Credentials{
		AccountID:      acctId,
		ApplicationKey: appKey,
		KeyID:          keyId,
	}

	if acctId != "" {
		creds.KeyID = ""
	}

	pool := &sync.Pool{
		New: func() interface{} {
			conn, err := backblaze.NewB2(creds)
			if err != nil {
				return err
			}
			bucket, err := conn.Bucket(bucketName)
			if err != nil {
				return err
			}
			return bucket
		},
	}
	return &b2{pool, url}, nil
}

// Fetches encrypted chunks from B2 using Backblaze's API. If a url is passed to
// the B2 constructor, this method instead attempts to fetch chunks from a file
// server at the configured url. Requesting data through configured urls does not
// support authentication and is limited to public buckets.
func (b *b2) Get(ctx context.Context, key string) ([]byte, error) {
	var resp io.ReadCloser
	var err error

	if b.url != "" {
		resp, err = getWithHostOverride(ctx, b.url, key)
	} else {
		resp, err = b.getWithAuth(key)
	}

	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			B2Ops.WithLabelValues("get", "true").Inc()
			return nil, err
		}

		B2Ops.WithLabelValues("get", "false").Inc()
		return nil, err
	}
	defer resp.Close()

	data, err := ioutil.ReadAll(resp)
	if err != nil {
		B2Ops.WithLabelValues("get", "false").Inc()
		return nil, err
	}

	B2Ops.WithLabelValues("get", "true").Inc()
	return data, nil
}

func (b *b2) Set(ctx context.Context, key string, data []byte, _ DataType) error {
	bucket := b.pool.Get()
	if err, ok := bucket.(error); ok {
		return err
	}
	defer b.pool.Put(bucket)

	meta := make(map[string]string)
	buff := bytes.NewReader(data)

	_, err := bucket.(*backblaze.Bucket).UploadTypedFile(key, "application/octet-string", meta, buff)
	if err != nil {
		B2Ops.WithLabelValues("set", "false").Inc()
		return err
	}

	B2Ops.WithLabelValues("set", "true").Inc()
	return nil
}

func (b *b2) Delete(ctx context.Context, key string) error {
	bucket := b.pool.Get()
	if err, ok := bucket.(error); ok {
		return err
	}
	defer b.pool.Put(bucket)

	if _, err := bucket.(*backblaze.Bucket).HideFile(key); err != nil {
		B2Ops.WithLabelValues("delete", "false").Inc()
		return err
	}

	B2Ops.WithLabelValues("delete", "true").Inc()
	return nil
}

func (b *b2) getWithAuth(key string) (io.ReadCloser, error) {
	bucket := b.pool.Get()
	if err, ok := bucket.(error); ok {
		return nil, err
	}
	defer b.pool.Put(bucket)

	_, reader, err := bucket.(*backblaze.Bucket).DownloadFileByName(key)
	if err != nil {
		if b2err, ok := err.(*backblaze.B2Error); ok {
			if b2err.Status == 404 {
				return nil, ErrObjectNotFound
			}
		}

		return nil, fmt.Errorf("storage: unexpected error: %v", err)
	}

	return reader, nil
}

func getWithHostOverride(ctx context.Context, domain, key string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%v/%v", domain, key), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 404 {
		return nil, ErrObjectNotFound
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("storage: unexpected response status: %v", resp.Status)
	}

	return resp.Body, nil
}
