package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"syscall"

	"github.com/cloudflare/utahfs"
	"github.com/cloudflare/utahfs/persistent"

	"golang.org/x/crypto/ssh/terminal"
	"gopkg.in/yaml.v2"
)

func maxSize(numPtrs, dataSize int64) int64 {
	//  8 = size of a single pointer
	//  3 = size of length field before data
	// 28 = encryption overhead
	return (8 * numPtrs) + (3 + dataSize) + 28
}

type StorageProvider struct {
	// Backblaze B2
	B2AcctId string `yaml:"b2-acct-id"`
	B2KeyId  string `yaml:"b2-key-id"`
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

func (sp *StorageProvider) hasB2() bool {
	return sp.B2AcctId != "" || sp.B2AppKey != "" || sp.B2Bucket != "" || sp.B2Url != ""
}

func (sp *StorageProvider) hasS3() bool {
	return sp.S3AppId != "" || sp.S3AppKey != "" || sp.S3Bucket != "" || sp.S3Url != "" || sp.S3Region != ""
}

func (sp *StorageProvider) hasGCS() bool {
	return sp.GCSBucketName != ""
}

func (sp *StorageProvider) hasDisk() bool { return sp.DiskPath != "" }

func (sp *StorageProvider) hasMultiple() bool {
	count := 0
	if sp.hasB2() {
		count++
	}
	if sp.hasS3() {
		count++
	}
	if sp.hasGCS() {
		count++
	}
	if sp.hasDisk() {
		count++
	}
	return count > 1
}

func (sp *StorageProvider) Store() (persistent.ObjectStorage, error) {
	if sp == nil || !sp.hasB2() && !sp.hasS3() && !sp.hasGCS() && !sp.hasDisk() {
		return nil, fmt.Errorf("no object storage provider defined")
	} else if sp.hasMultiple() {
		return nil, fmt.Errorf("only one object storage provider may be defined")
	}

	// Connect to the user's chosen storage provider.
	var (
		out persistent.ObjectStorage
		err error
	)
	if sp.hasB2() {
		out, err = persistent.NewB2(sp.B2AcctId, sp.B2KeyId, sp.B2AppKey, sp.B2Bucket, sp.B2Url)
	} else if sp.hasS3() {
		out, err = persistent.NewS3(sp.S3AppId, sp.S3AppKey, sp.S3Bucket, sp.S3Url, sp.S3Region)
	} else if sp.hasGCS() {
		out, err = persistent.NewGCS(sp.GCSBucketName, sp.GCSCredentialsPath)
	} else if sp.hasDisk() {
		out, err = persistent.NewDisk(sp.DiskPath)
	}
	if err != nil {
		return nil, err
	}

	// Configure retries if the user wants.
	if sp.Retry > 1 {
		out, err = persistent.NewRetry(out, sp.Retry)
		if err != nil {
			return nil, err
		}
	}
	// Configure a key prefix if the user wants.
	if sp.Prefix != "" {
		out = persistent.NewPrefix(out, sp.Prefix)
	}

	return out, nil
}

type RemoteServer struct {
	URL          string `yaml:"url"`           // URL of server.
	TransportKey string `yaml:"transport-key"` // Pre-shared key for authenticating client and server.
}

type Client struct {
	DataDir string `yaml:"data-dir"` // Directory where the WAL and pin file should be kept. Default: .utahfs

	StorageProvider *StorageProvider `yaml:"storage-provider"`
	MaxWALSize      int              `yaml:"max-wal-size"`    // Max number of blocks to put in WAL before blocking on remote storage. Default: 128*1024 blocks
	WALParallelism  int              `yaml:"wal-parallelism"` // Number of threads to use when draining the WAL. Default: 1
	DiskCacheSize   int64            `yaml:"disk-cache-size"` // Size of on-disk LRU cache. Default: 320*1024 blocks, -1 to disable.
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

func ClientFromFile(path string) (*Client, error) {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parsed := &Client{}
	if err = yaml.UnmarshalStrict(raw, parsed); err != nil {
		return nil, err
	}
	return parsed, nil
}

func (c *Client) localStorage() (persistent.ReliableStorage, error) {
	// Setup object storage.
	store, err := c.StorageProvider.Store()
	if err != nil {
		return nil, err
	}

	// Setup on-disk caching if desired.
	if c.DiskCacheSize == 0 {
		c.DiskCacheSize = 320 * 1024
	}
	if c.DiskCacheSize != -1 {
		loc := c.DiskCacheLoc
		if loc == "" {
			loc = path.Join(c.DataDir, "cache")
		}
		exclude := []persistent.DataType{persistent.Unknown}
		if c.KeepMetadata {
			exclude = append(exclude, persistent.Metadata)
		}
		store, err = persistent.NewDiskCache(store, loc, c.DiskCacheSize, exclude)
		if err != nil {
			return nil, err
		}
	}

	// Setup tiered caching for metadata if desired.
	if c.KeepMetadata {
		diskStore, err := persistent.NewDisk(path.Join(c.DataDir, "metadata"))
		if err != nil {
			return nil, err
		}
		store = persistent.NewTieredCache(persistent.Metadata, diskStore, store)
	}

	// Setup a local WAL.
	if c.MaxWALSize == 0 {
		c.MaxWALSize = 128 * 1024
	}
	if c.WALParallelism == 0 {
		c.WALParallelism = 1
	}
	relStore, err := persistent.NewLocalWAL(store, path.Join(c.DataDir, "wal"), c.MaxWALSize, c.WALParallelism)
	if err != nil {
		return nil, err
	}

	// Setup caching if desired.
	if c.MemCacheSize == 0 {
		c.MemCacheSize = 32 * 1024
	}
	if c.MemCacheSize != -1 {
		relStore = persistent.NewCache(relStore, c.MemCacheSize)
	}

	return relStore, nil
}

func (c *Client) remoteStorage() (persistent.ReliableStorage, error) {
	if c.StorageProvider != nil {
		return nil, fmt.Errorf("cannot set storage-provider with remote-server")
	} else if c.MaxWALSize != 0 {
		return nil, fmt.Errorf("cannot set max-wal-size with remote-server")
	} else if c.WALParallelism != 0 {
		return nil, fmt.Errorf("cannot set wal-parallelism with remote-server")
	} else if c.DiskCacheSize != 0 {
		return nil, fmt.Errorf("cannot set disk-cache-size with remote-server")
	} else if c.DiskCacheLoc != "" {
		return nil, fmt.Errorf("cannot set disk-cache-loc with remote-server")
	} else if c.MemCacheSize != 0 {
		return nil, fmt.Errorf("cannot set mem-cache-size with remote-server")
	} else if c.KeepMetadata {
		return nil, fmt.Errorf("cannot set keep-metadata with remote-server")
	} else if c.RemoteServer.TransportKey == "" {
		return nil, fmt.Errorf("no transport key was given for remote server")
	} else if c.RemoteServer.TransportKey == c.Password {
		return nil, fmt.Errorf("transport key should be generated independently of the encryption password")
	}
	return persistent.NewRemoteClient(c.RemoteServer.TransportKey, c.RemoteServer.URL, c.ORAM)
}

func (c *Client) FS(mountPath string) (*utahfs.BlockFilesystem, error) {
	if c.DataDir == "" {
		c.DataDir = path.Join(path.Dir(mountPath), ".utahfs")
	}

	// Stub out generation of the ReliableStorage interface, depending on if
	// this client is standalone or backed by a server.
	var (
		relStore persistent.ReliableStorage
		err      error
	)
	if c.RemoteServer == nil {
		relStore, err = c.localStorage()
	} else {
		relStore, err = c.remoteStorage()
	}
	if err != nil {
		return nil, err
	}

	// Setup buffered block storage.
	block := persistent.NewBufferedStorage(relStore)

	// Setup encryption and integrity.
	if c.Password == "" {
		fmt.Print("Password: ")
		password, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return nil, fmt.Errorf("failed reading password from stdin")
		} else if len(password) == 0 {
			return nil, fmt.Errorf("no password given for encryption")
		}
		c.Password = string(password)
	}
	if !c.ORAM || c.RemoteServer == nil {
		block, err = persistent.WithIntegrity(block, c.Password, path.Join(c.DataDir, "pin.json"))
		if err != nil {
			return nil, err
		}
	} else {
		log.Println("WARNING: delegating rollback prevention to remote server because ORAM is enabled")
	}
	block = persistent.WithEncryption(block, c.Password)

	// Configure defaults for the block-based filesystem. Do this early because
	// the numbers might be needed for ORAM.
	if c.NumPtrs == 0 {
		c.NumPtrs = 12
	}
	if c.DataSize == 0 {
		c.DataSize = 32 * 1024
	}

	// Setup ORAM if desired.
	if c.ORAM && c.RemoteServer == nil {
		if c.StorageProvider.hasDisk() {
			log.Println("WARNING: ORAM provides no security properties when used with disk storage")
		}
		ostore, err := persistent.NewLocalOblivious(path.Join(c.DataDir, "oram"))
		if err != nil {
			return nil, err
		}
		block, err = persistent.WithORAM(block, ostore, maxSize(c.NumPtrs, c.DataSize))
		if err != nil {
			return nil, err
		}
	}

	// Setup application storage.
	appStore := persistent.NewAppStorage(block)

	// Setup block-based filesystem.
	bfs, err := utahfs.NewBlockFilesystem(appStore, c.NumPtrs, c.DataSize, !c.ORAM)
	if err != nil {
		return nil, err
	}

	return bfs, nil
}

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
	DiskCacheSize  int64  `yaml:"disk-cache-size"` // Size of on-disk LRU cache. Default: 3200*1024 blocks, -1 to disable.
	DiskCacheLoc   string `yaml:"disk-cache-loc"`  // Special location for on-disk LRU cache. Default is to store cache inside data-dir.
	MemCacheSize   int    `yaml:"mem-cache-size"`  // Size of in-memory LRU cache. Default: 32*1024 blocks, -1 to disable.
	KeepMetadata   bool   `yaml:"keep-metadata"`   // Keep a local copy of metadata, always. Default: false.

	ORAM *ORAMConfig `yaml:"oram"` // Provided if ORAM should be used on the server-side.

	TransportKey string `yaml:"transport-key"` // Pre-shared key for authenticating client and server.
}

func ServerFromFile(path string) (*Server, error) {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parsed := &Server{}
	if err = yaml.UnmarshalStrict(raw, parsed); err != nil {
		return nil, err
	}
	return parsed, nil
}

func (s *Server) Server() (*http.Server, error) {
	if s.DataDir == "" {
		s.DataDir = "./utahfs-data"
	}

	// Setup object storage.
	store, err := s.StorageProvider.Store()
	if err != nil {
		return nil, err
	}

	// Setup on-disk caching if desired.
	if s.DiskCacheSize == 0 {
		s.DiskCacheSize = 3200 * 1024
	}
	if s.DiskCacheSize != -1 {
		loc := s.DiskCacheLoc
		if loc == "" {
			loc = path.Join(s.DataDir, "cache")
		}
		exclude := []persistent.DataType{persistent.Unknown}
		if s.KeepMetadata {
			exclude = append(exclude, persistent.Metadata)
		}
		store, err = persistent.NewDiskCache(store, loc, s.DiskCacheSize, exclude)
		if err != nil {
			return nil, err
		}
	}

	// Setup tiered caching for metadata, if desired.
	if s.KeepMetadata {
		diskStore, err := persistent.NewDisk(path.Join(s.DataDir, "metadata"))
		if err != nil {
			return nil, err
		}
		store = persistent.NewTieredCache(persistent.Metadata, diskStore, store)
	}

	// Setup a local WAL.
	if s.MaxWALSize == 0 {
		s.MaxWALSize = 32 * 1024
	}
	if s.WALParallelism == 0 {
		s.WALParallelism = 1
	}
	relStore, err := persistent.NewLocalWAL(store, path.Join(s.DataDir, "wal"), s.MaxWALSize, s.WALParallelism)
	if err != nil {
		return nil, err
	}

	// Setup in-memory caching if desired.
	if s.MemCacheSize == 0 {
		s.MemCacheSize = 32 * 1024
	}
	if s.MemCacheSize != -1 {
		relStore = persistent.NewCache(relStore, s.MemCacheSize)
	}

	// Setup ORAM if desired.
	if s.ORAM != nil {
		if s.StorageProvider.hasDisk() {
			log.Println("WARNING: ORAM provides no security properties when used with disk storage")
		}
		// Setup defaults.
		if s.ORAM.NumPtrs == 0 {
			s.ORAM.NumPtrs = 12
		}
		if s.ORAM.DataSize == 0 {
			s.ORAM.DataSize = 32 * 1024
		}

		ostore, err := persistent.NewLocalOblivious(path.Join(s.DataDir, "oram"))
		if err != nil {
			return nil, err
		}
		block, err := persistent.WithIntegrity(
			persistent.NewBufferedStorage(relStore),
			s.ORAM.Key,
			path.Join(s.DataDir, "pin.json"),
		)
		if err != nil {
			return nil, err
		}
		block, err = persistent.WithORAM(
			persistent.WithEncryption(block, s.ORAM.Key),
			ostore,
			maxSize(s.ORAM.NumPtrs, s.ORAM.DataSize),
		)
		if err != nil {
			return nil, err
		}
		relStore = persistent.NewBlockReliable(block)
	}

	// Setup the server we want to expose.
	if s.TransportKey == "" {
		return nil, fmt.Errorf("no transport key was given for remote clients")
	}
	return persistent.NewRemoteServer(relStore, s.TransportKey, s.ORAM != nil)
}
