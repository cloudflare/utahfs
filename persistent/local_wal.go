package persistent

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/prometheus/client_golang/prometheus"
)

var LocalWALSize = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "local_wal_size",
		Help: "The number of entries in the local WAL.",
	},
	[]string{"path"},
)

type localWAL struct {
	mu sync.Mutex

	base  ObjectStorage
	local *sql.DB

	loc     string
	maxSize int
	wake    chan struct{}

	currSize  int
	lastCount time.Time
}

// NewLocalWAL returns a ReliableStorage implementation that achieves reliable
// writes over a base object storage provider by buffering writes in a
// Write-Ahead Log (WAL) stored at `loc`.
//
// The WAL may have at least `maxSize` buffered entries before new writes start
// blocking on old writes being flushed.
func NewLocalWAL(base ObjectStorage, loc string, maxSize int) (ReliableStorage, error) {
	if err := os.MkdirAll(path.Dir(loc), 0744); err != nil {
		return nil, err
	}
	local, err := sql.Open("sqlite3", loc)
	if err != nil {
		return nil, err
	}
	_, err = local.Exec(`CREATE TABLE IF NOT EXISTS wal (id integer not null primary key, key text, val bytea); CREATE INDEX IF NOT EXISTS by_key ON wal(key);`)
	if err != nil {
		return nil, err
	}
	wal := &localWAL{
		base:  base,
		local: local,

		loc:     loc,
		maxSize: maxSize,
		wake:    make(chan struct{}),

		currSize:  0,
		lastCount: time.Time{},
	}
	go wal.drain()
	go func() {
		for {
			time.Sleep(10 * time.Second)
			wal.count()
		}
	}()

	return wal, nil
}

func (lw *localWAL) drain() {
	tick := time.Tick(5 * time.Second)

	for {
		select {
		case <-tick:
		case <-lw.wake:
		}

		if err := lw.drainOnce(); err != nil {
			log.Println(err)
		}
	}
}

func (lw *localWAL) drainOnce() error {
	for {
		var (
			ids  []string
			keys []string
			vals [][]byte
		)
		rows, err := lw.local.Query("SELECT id, key, val FROM wal LIMIT 100;")
		if err != nil {
			return err
		}

		for rows.Next() {
			var (
				id  int
				key string
				val []byte
			)
			if err := rows.Scan(&id, &key, &val); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, fmt.Sprint(id))
			keys = append(keys, key)
			vals = append(vals, val)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(ids) == 0 {
			return nil
		}

		// Write entries read from the WAL to the underlying storage. This is
		// done outside of the database query to prevent blocking other threads.
		for i, _ := range ids {
			key, val := keys[i], vals[i]
			if len(val) > 0 {
				if err := lw.base.Set(context.Background(), key, val); err != nil {
					return err
				}
			} else {
				if err := lw.base.Delete(context.Background(), key); err != nil {
					return err
				}
			}
		}

		_, err = lw.local.Exec("DELETE FROM wal WHERE id in (" + strings.Join(ids, ",") + ")")
		if err != nil {
			return err
		}
	}
}

func (lw *localWAL) count() (int, error) {
	lw.mu.Lock()
	if time.Since(lw.lastCount) < 10*time.Second {
		curr := lw.currSize
		lw.mu.Unlock()
		return curr, nil
	}
	lw.mu.Unlock()

	var count int
	err := lw.local.QueryRow("SELECT COUNT(*) FROM wal;").Scan(&count)
	if err != nil {
		return 0, err
	}

	lw.mu.Lock()
	lw.lastCount = time.Now()
	lw.currSize = count
	lw.mu.Unlock()

	LocalWALSize.WithLabelValues(lw.loc).Set(float64(count))
	return count, nil
}

func (lw *localWAL) Start(ctx context.Context) error {
	// Block until the database has drained enough to accept new writes.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		count, err := lw.count()
		if err != nil {
			return err
		}

		if count > lw.maxSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case lw.wake <- struct{}{}:
			case <-ticker.C:
			}
			continue
		}
		return nil
	}
}

func (lw *localWAL) Get(ctx context.Context, key string) ([]byte, error) {
	var val []byte
	err := lw.local.QueryRow("SELECT val FROM wal WHERE key = ?;", key).Scan(&val)
	if err == sql.ErrNoRows {
		return lw.base.Get(ctx, key)
	} else if len(val) == 0 {
		return nil, ErrObjectNotFound
	} else if err != nil {
		return nil, err
	}
	return val, nil
}

func (lw *localWAL) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte)
	for _, key := range keys {
		val, err := lw.Get(ctx, key)
		if err == ErrObjectNotFound {
			continue
		} else if err != nil {
			return nil, err
		}
		out[key] = val
	}
	return out, nil
}

func (lw *localWAL) Commit(ctx context.Context, writes map[string][]byte) error {
	if len(writes) == 0 {
		return nil
	}

	tx, err := lw.local.Begin()
	if err != nil {
		return err
	}
	delStmt, err := tx.Prepare("DELETE FROM wal WHERE key = ?;")
	if err != nil {
		tx.Rollback()
		return err
	}
	insertStmt, err := tx.Prepare("INSERT INTO wal (key, val) VALUES (?, ?);")
	if err != nil {
		tx.Rollback()
		return err
	}

	for key, val := range writes {
		if _, err := delStmt.Exec(key); err != nil {
			tx.Rollback()
			return err
		} else if _, err := insertStmt.Exec(key, val); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}
