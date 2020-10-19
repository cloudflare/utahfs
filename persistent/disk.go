package persistent

import (
	"context"
	"database/sql"
	"os"
	"path"

	_ "github.com/mattn/go-sqlite3"
)

type disk struct {
	db *sql.DB
}

// NewDisk returns object storage backed by an on-disk database stored at `loc`.
func NewDisk(loc string) (ObjectStorage, error) {
	if err := os.MkdirAll(path.Dir(loc), 0744); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", loc)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS db (key text not null primary key, val bytea)")
	if err != nil {
		return nil, err
	}

	return &disk{db}, nil
}

func (d *disk) Get(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	err := d.db.QueryRowContext(ctx, "SELECT val FROM db WHERE key = ?", key).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, ErrObjectNotFound
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func (d *disk) Set(ctx context.Context, key string, data []byte, _ DataType) error {
	_, err := d.db.ExecContext(ctx, "INSERT OR REPLACE INTO db (key, val) VALUES (?, ?)", key, data)
	return err
}

func (d *disk) Delete(ctx context.Context, key string) error {
	_, err := d.db.ExecContext(ctx, "DELETE FROM db WHERE key = ?", key)
	return err
}
