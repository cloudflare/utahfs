Advanced Configuration
----------------------

This document describes the full range of configuration options for UtahFS.

### Storage Provider Configuration

`StorageProvider` is a sub-section of both client-side and server-side config
files.

```go
type StorageProvider struct {
	// Backblaze B2
	B2AcctId string `yaml:"b2-acct-id"`
	B2AppKey string `yaml:"b2-app-key"`
	B2Bucket string `yaml:"b2-bucket"`
	B2Url    string `yaml:"b2-url"`

	// AWS S3 and compatible APIs
	S3AppId  string `yaml:"s3-app-id"`
	S3AppKey string `yaml:"s3-app-key"`
	S3Bucket string `yaml:"s3-bucket"`
	S3Url    string `yaml:"s3-url"`
	S3Region string `yaml:"s3-region"`

	// Google Cloud Storage
	GCSBucketName      string `yaml:"gcs-bucket-name"`
	GCSCredentialsPath string `yaml:"gcs-credentials-path"`

	// Local disk storage
	DiskPath string `yaml:"disk-path"`

	Retry  int    `yaml:"retry"`  // Max number of times to retry reqs that fail.
	Prefix string `yaml:"prefix"` // Prefix to put on every key, like `folder-name/`.
}
```

Only one section of the `StorageProvider` object may be set, corresponding to
one storage provider, along with an optional `retry` count to reduce sporadic
failures or a key prefix.


### Client Config

The `Client` structure corresponds to the body of a client-side config file.

```go
type RemoteServer struct {
	URL          string `yaml:"url"`           // URL of server.
	TransportKey string `yaml:"transport-key"` // Pre-shared key for authenticating client and server.
}

type Client struct {
	DataDir string `yaml:"data-dir"` // Directory where the WAL and pin file should be kept. Default: .utahfs

	StorageProvider *StorageProvider `yaml:"storage-provider"`
	MaxWALSize      int              `yaml:"max-wal-size"`    // Max number of blocks to put in WAL before blocking on remote storage. Default: 128*1024 blocks
	WALParallelism  int              `yaml:"wal-parallelism"` // Number of threads to use when draining the WAL. Default: 1
	DiskCacheSize   int              `yaml:"disk-cache-size"` // Size of on-disk LRU cache. Default: 320*1024 blocks, -1 to disable.
	DiskCacheLoc    string           `yaml:"disk-cache-loc"`  // Special location for on-disk LRU cache. Default is to store cache inside data-dir.
	MemCacheSize    int              `yaml:"mem-cache-size"`  // Size of in-memory LRU cache. Default: 32*1024 blocks, -1 to disable.
	KeepMetadata    bool             `yaml:"keep-metadata"`   // Keep a local copy of metadata, always. Default: false.

	RemoteServer *RemoteServer `yaml:"remote-server"`

	Password string `yaml:"password"` // Password for encryption and integrity. User will be prompted if not provided.

	NumPtrs  int64 `yaml:"num-ptrs"`  // Number of pointers in a file's skiplist. Default: 12
	DataSize int64 `yaml:"data-size"` // Amount of data kept in each of a file's blocks. Default: 32 KiB

	Archive bool `yaml:"archive"` // Whether or not to enforce archive mode.
	ORAM    bool `yaml:"oram"`    // Whether or not to use ORAM.
}
```

A `remote-server` section in the config file indicates that we're in
Multi-Device mode, in which case none of the config settings `storage-provider`,
`max-wal-size`, ..., through `keep-metadata` are allowed to be set.

Increasing the size of data blocks by raising the `data-size` config setting can
improve the performance of applications like video streaming, where we benefit
from needing fewer requests to buffer data. The trade-off is that things like
inodes and small files will be padded to this larger block size, wasting space
and bandwidth when they need to be accessed. Note that there is one inode per
file or folder. It's not recommended to change this setting drastically from the
default.


### Server Config

The `Server` structure corresponds to the body of a server-side config file.

```go
type ORAMConfig struct {
	Key string `yaml:"key"` // Fixed key for encrypting ORAM blocks before being sent to the remote storage provider.

	NumPtrs  int64 `yaml:"num-ptrs"`  // Should be the same as num-ptrs in the client-side config.
	DataSize int64 `yaml:"data-size"` // Should be the same as data-size in the client-side config.
}

type Server struct {
	DataDir string `yaml:"data-dir"` // Directory where the WAL and cache should be kept. Default: utahfs-data

	StorageProvider *StorageProvider `yaml:"storage-provider"`

	MaxWALSize     int    `yaml:"max-wal-size"`    // Max number of blocks to put in WAL before blocking on remote storage. Default: 320*1024 blocks
	WALParallelism int    `yaml:"wal-parallelism"` // Number of threads to use when draining the WAL. Default: 1
	DiskCacheSize  int    `yaml:"disk-cache-size"` // Size of on-disk LRU cache. Default: 3200*1024 blocks, -1 to disable.
	DiskCacheLoc   string `yaml:"disk-cache-loc"`  // Special location for on-disk LRU cache. Default is to store cache inside data-dir.
	MemCacheSize   int    `yaml:"mem-cache-size"`  // Size of in-memory LRU cache. Default: 32*1024 blocks, -1 to disable.
	KeepMetadata   bool   `yaml:"keep-metadata"`   // Keep a local copy of metadata, always. Default: false.

	ORAM *ORAMConfig `yaml:"oram"` // Provided if ORAM should be used on the server-side.

	TransportKey string `yaml:"transport-key"` // Pre-shared key for authenticating client and server.
}
```

When a server is being used, it's often valuable to fine-tune the caching of
data blocks. The cache may be stored on a different disk than the rest of the
archive's local data, by setting the `disk-cache-loc` setting. You'll also
likely want to adjust the number of blocks stored in cache, by setting the
`disk-cache-size` setting. Assume the average block size is about `data-size`
bytes.
