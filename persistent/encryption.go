package persistent

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

type encryption struct {
	base BlockStorage
	aead cipher.AEAD
}

// WithEncryption wraps a BlockStorage implementation and makes sure that all
// values are encrypted with AES-GCM before being processed further.
//
// The encryption key is derived with PBKDF2 from `password`.
func WithEncryption(base BlockStorage, password string) (BlockStorage, error) {
	key := pbkdf2.Key([]byte(password), []byte("7fedd6d671beec56"), 4096, 32, sha1.New)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &encryption{base, aead}, nil
}

func (e *encryption) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	if len(prefetch) > 0 {
		return nil, fmt.Errorf("encryption: prefetch is not supported")
	}
	return e.base.Start(ctx, nil)
}

func (e *encryption) Get(ctx context.Context, ptr uint64) ([]byte, error) {
	data, err := e.GetMany(ctx, []uint64{ptr})
	if err != nil {
		return nil, err
	} else if data[ptr] == nil {
		return nil, ErrObjectNotFound
	}
	return data[ptr], nil
}

func (e *encryption) GetMany(ctx context.Context, ptrs []uint64) (map[uint64][]byte, error) {
	data, err := e.base.GetMany(ctx, ptrs)
	if err != nil {
		return nil, err
	}

	ns := e.aead.NonceSize()
	out := make(map[uint64][]byte)
	for ptr, raw := range data {
		if len(raw) < ns {
			return nil, fmt.Errorf("storage: ciphertext is too small")
		}
		val, err := e.aead.Open(nil, raw[:ns], raw[ns:], []byte(fmt.Sprintf("%x", ptr)))
		if err != nil {
			return nil, err
		}
		out[ptr] = val
	}
	return out, nil
}

func (e *encryption) Set(ctx context.Context, ptr uint64, data []byte, dt DataType) error {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	ct := e.aead.Seal(nil, nonce, data, []byte(fmt.Sprintf("%x", ptr)))

	return e.base.Set(ctx, ptr, append(nonce, ct...), dt)
}

func (e *encryption) Commit(ctx context.Context) error { return e.base.Commit(ctx) }
func (e *encryption) Rollback(ctx context.Context)     { e.base.Rollback(ctx) }
