package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/Bren2010/utahfs"
	"gopkg.in/kothar/go-backblaze.v0"
)

var (
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

// NewB2 returns a new object storage backend, where `acctId` and `appKey` are
// the Account ID and Application Key of a B2 bucket. `bucket` is the name of
// the bucket. `url` is the URL to use to download data.
func NewB2(acctId, appKey, bucket, url string) (utahfs.ObjectStorage, error) {
	conn, err := backblaze.NewB2(backblaze.Credentials{
		AccountID:      acctId,
		ApplicationKey: appKey,
	})
	if err != nil {
		return nil, err
	}
	return &b2{conn, bucket, url}, nil
}

func (b *b2) Get(key string) ([]byte, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%v/%v", b.url, key), nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, utahfs.ErrObjectNotFound
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected response status: %v", resp.Status)
	}

	return ioutil.ReadAll(resp.Body)
}

func (b *b2) Set(key string, data []byte) error {
	meta := make(map[string]string)
	buff := bytes.NewBuffer(data)

	bucket, err := b.conn.Bucket(b.bucket)
	if err != nil {
		return err
	}
	if _, err := bucket.UploadFile(key, meta, buff); err != nil {
		return err
	}

	return nil
}

func (b *b2) Delete(key string) error {
	bucket, err := b.conn.Bucket(b.bucket)
	if err != nil {
		return err
	}
	if _, err := bucket.HideFile(key); err != nil {
		return err
	}

	return nil
}
