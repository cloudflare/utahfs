package persistent

import (
	"context"
	"io/ioutil"
	"os"

	"cloud.google.com/go/storage"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	GCSOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gcs_ops",
			Help: "The number of operations against a GCS backend.",
		},
		[]string{"operation", "success"},
	)
)

type gcs struct {
	bucket *storage.BucketHandle
}

// NewGCS returns object storage backed by Google Compute Storage. `bucketName`
// is the name of the bucket to use. Authentication credentials should be stored
// in a file, and the path to that file is `credentialsPath`.
func NewGCS(bucketName, credentialsPath string) (ObjectStorage, error) {
	if credentialsPath != "" {
		if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialsPath); err != nil {
			return nil, err
		}
	}

	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(bucketName)

	return &gcs{bucket}, nil
}

func (g *gcs) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := g.bucket.Object(key).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		GCSOps.WithLabelValues("get", "true").Inc()
		return nil, ErrObjectNotFound
	} else if err != nil {
		GCSOps.WithLabelValues("get", "false").Inc()
		return nil, err
	}
	defer r.Close()

	data, err := ioutil.ReadAll(r)
	if err != nil {
		GCSOps.WithLabelValues("get", "false").Inc()
		return nil, err
	}
	GCSOps.WithLabelValues("get", "true").Inc()
	return data, nil
}

func (g *gcs) Set(ctx context.Context, key string, data []byte, _ DataType) error {
	w := g.bucket.Object(key).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		GCSOps.WithLabelValues("set", "false").Inc()
		return err
	} else if err := w.Close(); err != nil {
		GCSOps.WithLabelValues("set", "false").Inc()
		return err
	}
	GCSOps.WithLabelValues("set", "true").Inc()
	return nil
}

func (g *gcs) Delete(ctx context.Context, key string) error {
	if err := g.bucket.Object(key).Delete(ctx); err != nil {
		GCSOps.WithLabelValues("delete", "false").Inc()
		return err
	}
	GCSOps.WithLabelValues("delete", "true").Inc()
	return nil
}
