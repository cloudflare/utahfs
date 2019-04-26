package storage

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/Bren2010/utahfs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Client struct {
	bucket string
	client *s3.S3
}

// NewS3 returns a new object storage backend, where
func NewS3(appId, appKey, bucket, url, region string) (utahfs.ObjectStorage, error) {
	client := s3.New(session.New(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(appId, appKey, ""),
		Endpoint:         aws.String(url),
		Region:           aws.String(region),
		S3ForcePathStyle: aws.Bool(true),
	}))

	return &s3Client{bucket, client}, nil
}

func (s *s3Client) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
		return nil, utahfs.ErrObjectNotFound
	} else if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(res.Body)
}

func (s *s3Client) Set(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *s3Client) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}
