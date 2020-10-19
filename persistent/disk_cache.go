package persistent

import (
	"context"
	"database/sql"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/prometheus/client_golang/prometheus"
)

var DiskCacheSize = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "disk_cache_size",
		Help: "The number of entries in the on-disk cache.",
	},
	[]string{"path"},
)

type diskCache struct {
	mu    sync.Mutex
	mapMu MapMutex

	base    ObjectStorage
	loc     string
	size    int64
	exclude []DataType

	n  int64
	db *sql.DB
}

// NewDiskCache wraps a base object storage backend with a large on-disk cache
// stored at `loc`.
func NewDiskCache(base ObjectStorage, loc string, size int64, exclude []DataType) (ObjectStorage, error) {
	if err := os.MkdirAll(path.Dir(loc), 0744); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", loc)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS cache (key text not null primary key, val bytea)")
	if err != nil {
		return nil, err
	}

	// Get the max rowid in the cache.
	var n *int64
	err = db.QueryRow("SELECT MAX(rowid) FROM cache").Scan(&n)
	if err != nil {
		return nil, err
	} else if n == nil {
		n = new(int64)
	}

	DiskCacheSize.WithLabelValues(loc).Set(float64(*n))
	return &diskCache{
		mapMu: NewMapMutex(),

		base:    base,
		loc:     loc,
		size:    size,
		exclude: exclude,

		n:  *n,
		db: db,
	}, nil
}

func (dc *diskCache) addToCache(ctx context.Context, key string, data []byte) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	tx, err := dc.db.BeginTx(ctx, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer tx.Rollback()

	n := dc.n + 1
	i := rand.Int63n(n) + 1

	// Move a random existing entry into the next rowid slot.
	if i != dc.n {
		_, err := tx.ExecContext(ctx, "UPDATE cache SET rowid = ? WHERE rowid = ?", n, i)
		if err != nil {
			log.Println(err)
			return
		}
	}
	// Add the new row to the cache.
	_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO cache (rowid, key, val) VALUES (?, ?, ?)", i, key, data)
	if err != nil {
		log.Println(err)
		return
	}
	// Evict from the cache until we're back at/below the target size.
	for n > dc.size {
		if _, err := tx.ExecContext(ctx, "DELETE FROM cache WHERE rowid = ?", n); err != nil {
			log.Println(err)
			return
		}
		n -= 1
	}

	// Commit the transaction.
	if err := tx.Commit(); err != nil {
		log.Println(err)
		return
	}
	dc.n = n
	DiskCacheSize.WithLabelValues(dc.loc).Set(float64(n))
}

func (dc *diskCache) removeFromCache(ctx context.Context, key string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	tx, err := dc.db.BeginTx(ctx, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer tx.Rollback()

	// Get the rowid of the key we want to delete.
	var rowid int64
	err = tx.QueryRowContext(ctx, "SELECT rowid FROM cache WHERE key = ?", key).Scan(&rowid)
	if err == sql.ErrNoRows {
		return
	} else if err != nil {
		log.Println(err)
		return
	}
	// Delete the row.
	if _, err := tx.ExecContext(ctx, "DELETE FROM cache WHERE rowid = ?", rowid); err != nil {
		log.Println(err)
		return
	}
	// Move something into this rowid gap.
	if _, err := tx.ExecContext(ctx, "UPDATE cache SET rowid = ? WHERE rowid = ?", rowid, dc.n); err != nil {
		log.Println(err)
		return
	}

	// Commit the transaction.
	if err := tx.Commit(); err != nil {
		log.Println(err)
		return
	}
	dc.n -= 1
	DiskCacheSize.WithLabelValues(dc.loc).Set(float64(dc.n))
}

func (dc *diskCache) Get(ctx context.Context, key string) ([]byte, error) {
	dc.mapMu.Lock(key)
	defer dc.mapMu.Unlock(key)

	var data []byte
	dc.mu.Lock()
	err := dc.db.QueryRowContext(ctx, "SELECT val FROM cache WHERE key = ?", key).Scan(&data)
	dc.mu.Unlock()
	if err == sql.ErrNoRows {
		data, err = dc.base.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		dc.addToCache(ctx, key, data)
		return data, nil
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func (dc *diskCache) Set(ctx context.Context, key string, data []byte, dt DataType) error {
	dc.mapMu.Lock(key)
	defer dc.mapMu.Unlock(key)

	if err := dc.base.Set(ctx, key, data, dt); err != nil {
		dc.removeFromCache(ctx, key)
		return err
	}

	// Check if this key has an excluded data type and skip caching if so.
	for _, cand := range dc.exclude {
		if dt == cand {
			return nil
		}
	}
	// Type isn't excluded; good to cache.
	dc.addToCache(ctx, key, data)
	return nil
}

func (dc *diskCache) Delete(ctx context.Context, key string) error {
	dc.mapMu.Lock(key)
	defer dc.mapMu.Unlock(key)

	err := dc.base.Delete(ctx, key)
	dc.removeFromCache(ctx, key)
	return err
}
