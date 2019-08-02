package config

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"syscall"

	"code.cfops.it/~brendan/utahfs"
	"code.cfops.it/~brendan/utahfs/persistent"

	"golang.org/x/crypto/ssh/terminal"
	"gopkg.in/yaml.v2"
)

type StorageProvider struct {
	B2AcctId string `yaml:"b2-acct-id"`
	B2AppKey string `yaml:"b2-app-key"`
	B2Bucket string `yaml:"b2-bucket"`
	B2Url    string `yaml:"b2-url"`

	S3AppId  string `yaml:"s3-app-id"`
	S3AppKey string `yaml:"s3-app-key"`
	S3Bucket string `yaml:"s3-bucket"`
	S3Url    string `yaml:"s3-url"`
	S3Region string `yaml:"s3-region"`

	Retry int `yaml:"retry"` // Max number of times to retry reqs that fail.
}

func (sp *StorageProvider) hasB2() bool {
	return sp.B2AcctId != "" || sp.B2AppKey != "" || sp.B2Bucket != "" || sp.B2Url != ""
}

func (sp *StorageProvider) hasS3() bool {
	return sp.S3AppId != "" || sp.S3AppKey != "" || sp.S3Bucket != "" || sp.S3Url != "" || sp.S3Region != ""
}

func (sp *StorageProvider) Store() (persistent.ObjectStorage, error) {
	if sp == nil || !sp.hasB2() && !sp.hasS3() {
		return nil, fmt.Errorf("no object storage provider defined")
	} else if sp.hasB2() && sp.hasS3() {
		return nil, fmt.Errorf("only one object storage provider may be defined")
	}

	// Connect to either B2 or S3.
	var (
		out persistent.ObjectStorage
		err error
	)
	if sp.hasB2() {
		out, err = persistent.NewB2(sp.B2AcctId, sp.B2AppKey, sp.B2Bucket, sp.B2Url)
	} else if sp.hasS3() {
		out, err = persistent.NewS3(sp.S3AppId, sp.S3AppKey, sp.S3Bucket, sp.S3Url, sp.S3Region)
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
	DiskCacheSize   int              `yaml:"disk-cache-size"` // Size of on-disk LRU cache. Default: 3200*1024 blocks, -1 to disable.
	MemCacheSize    int              `yaml:"mem-cache-size"`  // Size of in-memory LRU cache. Default: 32*1024 blocks, -1 to disable.

	RemoteServer *RemoteServer `yaml:"remote-server"`

	Password string `yaml:"password"` // Password for encryption and integrity. User will be prompted if not provided.

	NumPtrs  int64 `yaml:"num-ptrs"`  // Number of pointers in a file's skiplist. Default: 12
	DataSize int64 `yaml:"data-size"` // Amount of data kept in each of a file's blocks. Default: 32 KiB

	Archive bool `yaml:"archive"` // Whether or not to enforce archive mode.
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
		c.DiskCacheSize = 3200 * 1024
	}
	if c.DiskCacheSize != -1 {
		store, err = persistent.NewDiskCache(store, path.Join(c.DataDir, "cache"), c.DiskCacheSize)
		if err != nil {
			return nil, err
		}
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
		relStore, err = persistent.NewCache(relStore, c.MemCacheSize)
		if err != nil {
			return nil, err
		}
	}

	return relStore, nil
}

func (c *Client) remoteStorage() (persistent.ReliableStorage, error) {
	if c.StorageProvider != nil {
		return nil, fmt.Errorf("cannot set storage-provider with remote-server")
	} else if c.MaxWALSize != 0 {
		return nil, fmt.Errorf("cannot set max-wal-size with remote-server")
	} else if c.DiskCacheSize != 0 {
		return nil, fmt.Errorf("cannot set disk-cache-size along with remote-server")
	} else if c.MemCacheSize != 0 {
		return nil, fmt.Errorf("cannot set mem-cache-size along with remote-server")
	} else if c.RemoteServer.TransportKey == "" {
		return nil, fmt.Errorf("no transport key was given for remote server")
	} else if c.RemoteServer.TransportKey == c.Password {
		return nil, fmt.Errorf("transport key should be generated independently of the encryption password")
	}
	return persistent.NewRemoteClient(c.RemoteServer.TransportKey, c.RemoteServer.URL)
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
	buffered := persistent.NewBufferedStorage(relStore)
	block := persistent.NewSimpleBlock(buffered)

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
	block, err = persistent.WithIntegrity(block, c.Password, path.Join(c.DataDir, "pin.json"))
	if err != nil {
		return nil, err
	}
	block, err = persistent.WithEncryption(block, c.Password)
	if err != nil {
		return nil, err
	}

	// Setup application storage.
	appStore := persistent.NewAppStorage(block)

	// Setup block-based filesystem.
	if c.NumPtrs == 0 {
		c.NumPtrs = 12
	}
	if c.DataSize == 0 {
		c.DataSize = 32 * 1024
	}
	bfs, err := utahfs.NewBlockFilesystem(appStore, c.NumPtrs, c.DataSize)
	if err != nil {
		return nil, err
	}

	return bfs, nil
}

type Server struct {
	DataDir string `yaml:"data-dir"` // Directory where the WAL and cache should be kept. Default: utahfs-data

	StorageProvider *StorageProvider `yaml:"storage-provider"`

	MaxWALSize     int `yaml:"max-wal-size"`    // Max number of blocks to put in WAL before blocking on remote storage. Default: 320*1024 blocks
	WALParallelism int `yaml:"wal-parallelism"` // Number of threads to use when draining the WAL. Default: 1
	DiskCacheSize  int `yaml:"disk-cache-size"` // Size of on-disk LRU cache. Default: 3200*1024 blocks, -1 to disable.
	MemCacheSize   int `yaml:"mem-cache-size"`  // Size of in-memory LRU cache. Default: 32*1024 blocks, -1 to disable.

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
		store, err = persistent.NewDiskCache(store, path.Join(s.DataDir, "cache"), s.DiskCacheSize)
		if err != nil {
			return nil, err
		}
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
		relStore, err = persistent.NewCache(relStore, s.MemCacheSize)
		if err != nil {
			return nil, err
		}
	}

	// Setup the server we want to expose.
	if s.TransportKey == "" {
		return nil, fmt.Errorf("no transport key was given for remote clients")
	}
	return persistent.NewRemoteServer(relStore, s.TransportKey)
}
