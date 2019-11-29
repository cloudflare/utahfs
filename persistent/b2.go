package persistent

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
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
	conn   *backblaze.B2
	bucket string
	url    string
}

// NewB2 returns object storage backed by Backblaze B2. `acctId` and `appKey`
// are the Account ID and Application Key of a B2 bucket. `bucket` is the name
// of the bucket. `url` is the URL to use to download data.
func NewB2(acctId, appKey, bucket, url string) (ObjectStorage, error) {
	conn, err := backblaze.NewB2(backblaze.Credentials{
		AccountID:      acctId,
		ApplicationKey: appKey,
	})
	if err != nil {
		return nil, err
	}
	return &b2{conn, bucket, url}, nil
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

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		B2Ops.WithLabelValues("get", "false").Inc()
		return nil, err
	}
	B2Ops.WithLabelValues("get", "true").Inc()
	return data, nil
}

func (b *b2) Set(ctx context.Context, key string, data []byte, _ DataType) error {
	meta := make(map[string]string)
	buff := bytes.NewBuffer(data)

	bucket, err := b.conn.Bucket(b.bucket)
	if err != nil {
		B2Ops.WithLabelValues("set", "false").Inc()
		return err
	} else if _, err := bucket.UploadFile(key, meta, buff); err != nil {
		B2Ops.WithLabelValues("set", "false").Inc()
		return err
	}

	B2Ops.WithLabelValues("set", "true").Inc()
	return nil
}

func (b *b2) Delete(ctx context.Context, key string) error {
	bucket, err := b.conn.Bucket(b.bucket)
	if err != nil {
		B2Ops.WithLabelValues("delete", "false").Inc()
		return err
	} else if _, err := bucket.HideFile(key); err != nil {
		B2Ops.WithLabelValues("delete", "false").Inc()
		return err
	}

	B2Ops.WithLabelValues("delete", "true").Inc()
	return nil
}
