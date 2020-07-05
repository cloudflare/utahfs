package persistent

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
// name of the bucket. `url` is the URL to use to download data.
func NewB2(acctId, appKey, bucketName, url string) (ObjectStorage, error) {
	pool := &sync.Pool{
		New: func() interface{} {
			conn, err := backblaze.NewB2(backblaze.Credentials{
				AccountID:      acctId,
				ApplicationKey: appKey,
			})
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

func (b *b2) Get(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%v/%v", b.url, key), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		B2Ops.WithLabelValues("get", "false").Inc()
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		B2Ops.WithLabelValues("get", "true").Inc()
		return nil, ErrObjectNotFound
	} else if resp.StatusCode != 200 {
		B2Ops.WithLabelValues("get", "false").Inc()
		return nil, fmt.Errorf("storage: unexpected response status: %v", resp.Status)
	}

	data := &bytes.Buffer{}
	_, err = io.Copy(data, resp.Body)
	if err != nil {
		B2Ops.WithLabelValues("get", "false").Inc()
		return nil, err
	}
	B2Ops.WithLabelValues("get", "true").Inc()
	return data.Bytes(), nil
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
