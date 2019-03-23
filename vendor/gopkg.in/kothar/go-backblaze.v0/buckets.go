package backblaze

import (
	"errors"
	"net/url"
)

// Bucket provides access to the files stored in a B2 Bucket
type Bucket struct {
	*BucketInfo

	uploadAuthPool chan *UploadAuth

	b2 *B2
}

// UploadAuth encapsulates the details needed to upload a file to B2
// These are pooled and must be returned when complete.
type UploadAuth struct {
	AuthorizationToken string
	UploadURL          *url.URL
	Valid              bool
}

// CreateBucket creates a new B2 Bucket in the authorized account.
//
// Buckets can be named. The name must be globally unique. No account can use
// a bucket with the same name. Buckets are assigned a unique bucketId which
// is used when uploading, downloading, or deleting files.
func (b *B2) CreateBucket(bucketName string, bucketType BucketType) (*Bucket, error) {
	return b.CreateBucketWithInfo(bucketName, bucketType, nil, nil)
}

// CreateBucketWithInfo extends CreateBucket to add bucket info and lifecycle rules to the creation request
func (b *B2) CreateBucketWithInfo(bucketName string, bucketType BucketType, bucketInfo map[string]string, lifecycleRules []LifecycleRule) (*Bucket, error) {
	request := &createBucketRequest{
		AccountID:      b.AccountID,
		BucketName:     bucketName,
		BucketType:     bucketType,
		BucketInfo:     bucketInfo,
		LifecycleRules: lifecycleRules,
	}
	response := &BucketInfo{}

	if err := b.apiRequest("b2_create_bucket", request, response); err != nil {
		return nil, err
	}

	bucket := &Bucket{
		BucketInfo:     response,
		uploadAuthPool: make(chan *UploadAuth, b.MaxIdleUploads),
		b2:             b,
	}

	return bucket, nil
}

// deleteBucket removes the specified bucket from the authorized account. Only
// buckets that contain no version of any files can be deleted.
func (b *B2) deleteBucket(bucketID string) (*Bucket, error) {
	request := &deleteBucketRequest{
		AccountID: b.AccountID,
		BucketID:  bucketID,
	}
	response := &BucketInfo{}

	if err := b.apiRequest("b2_delete_bucket", request, response); err != nil {
		return nil, err
	}

	return &Bucket{
		BucketInfo:     response,
		uploadAuthPool: make(chan *UploadAuth, b.MaxIdleUploads),
		b2:             b,
	}, nil
}

// Delete removes removes the bucket from the authorized account. Only buckets
// that contain no version of any files can be deleted.
func (b *Bucket) Delete() error {
	_, error := b.b2.deleteBucket(b.ID)
	return error
}

// ListBuckets lists buckets associated with an account, in alphabetical order
// by bucket ID.
func (b *B2) ListBuckets() ([]*Bucket, error) {
	request := &accountRequest{
		ID: b.AccountID,
	}
	response := &listBucketsResponse{}

	if err := b.apiRequest("b2_list_buckets", request, response); err != nil {
		return nil, err
	}

	// Construct bucket list
	buckets := make([]*Bucket, len(response.Buckets))
	for i, info := range response.Buckets {
		bucket := &Bucket{
			BucketInfo:     info,
			uploadAuthPool: make(chan *UploadAuth, b.MaxIdleUploads),
			b2:             b,
		}

		switch info.BucketType {
		case AllPublic:
		case AllPrivate:
		case Snapshot:
		default:
			return nil, errors.New("Uncrecognised bucket type: " + string(bucket.BucketType))
		}

		buckets[i] = bucket
	}

	return buckets, nil
}

// UpdateBucket allows properties of a bucket to be modified
func (b *B2) updateBucket(request *updateBucketRequest) (*Bucket, error) {
	response := &BucketInfo{}

	if err := b.apiRequest("b2_update_bucket", request, response); err != nil {
		return nil, err
	}

	return &Bucket{
		BucketInfo:     response,
		uploadAuthPool: make(chan *UploadAuth, b.MaxIdleUploads),
		b2:             b,
	}, nil
}

// Update allows the bucket type to be changed
func (b *Bucket) Update(bucketType BucketType) error {
	return b.UpdateAll(bucketType, nil, nil, 0)
}

// UpdateAll allows all bucket properties to be changed
//
// bucketType (optional) -- One of: "allPublic", "allPrivate". "allPublic" means that anybody can download the files is the bucket;
// "allPrivate" means that you need an authorization token to download them.
// If not specified, setting will remain unchanged.
//
// bucketInfo (optional) -- User-defined information to be stored with the bucket.
// If specified, the existing bucket info will be replaced with the new info. If not specified, setting will remain unchanged.
// Cache-Control policies can be set here on a global level for all the files in the bucket.
//
// lifecycleRules (optional) --  The list of lifecycle rules for this bucket.
// If specified, the existing lifecycle rules will be replaced with this new list. If not specified, setting will remain unchanged.
//
// ifRevisionIs (optional) -- When set (> 0), the update will only happen if the revision number stored in the B2 service matches the one passed in.
// This can be used to avoid having simultaneous updates make conflicting changes.
func (b *Bucket) UpdateAll(bucketType BucketType, bucketInfo map[string]string, lifecycleRules []LifecycleRule, ifRevisionIs int) error {
	_, err := b.b2.updateBucket(&updateBucketRequest{
		AccountID:      b.AccountID,
		BucketID:       b.ID,
		BucketType:     bucketType,
		BucketInfo:     bucketInfo,
		LifecycleRules: lifecycleRules,
		IfRevisionIs:   ifRevisionIs,
	})
	return err
}

// Bucket looks up a bucket for the currently authorized client
func (b *B2) Bucket(bucketName string) (*Bucket, error) {
	buckets, err := b.ListBuckets()
	if err != nil {
		return nil, err
	}

	for _, bucket := range buckets {
		if bucket.Name == bucketName {
			return bucket, nil
		}
	}

	return nil, nil
}

// GetUploadAuth retrieves the URL to use for uploading files.
//
// When you upload a file to B2, you must call b2_get_upload_url first to get
// the URL for uploading directly to the place where the file will be stored.
//
// If the upload is successful, ReturnUploadAuth(*uploadAuth) should be called
// to place it back in the pool for reuse.
func (b *Bucket) GetUploadAuth() (*UploadAuth, error) {
	select {
	// Pop an UploadAuth from the pool
	case auth := <-b.uploadAuthPool:
		return auth, nil

	// If none are available, make a new one
	default:
		// Make a new one
		request := &bucketRequest{
			ID: b.ID,
		}

		response := &getUploadURLResponse{}
		if err := b.b2.apiRequest("b2_get_upload_url", request, response); err != nil {
			return nil, err
		}

		// Set bucket auth
		uploadURL, err := url.Parse(response.UploadURL)
		if err != nil {
			return nil, err
		}
		auth := &UploadAuth{
			AuthorizationToken: response.AuthorizationToken,
			UploadURL:          uploadURL,
			Valid:              true,
		}

		return auth, nil
	}
}

// ReturnUploadAuth returns an upload URL to the available pool.
// This should not be called if the upload fails.
// Instead request another GetUploadAuth() and retry.
func (b *Bucket) ReturnUploadAuth(uploadAuth *UploadAuth) {
	if uploadAuth.Valid {
		select {
		case b.uploadAuthPool <- uploadAuth:
		default:
		}
	}
}
