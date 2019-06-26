package storage

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"fmt"

	"github.com/Bren2010/utahfs"

	"golang.org/x/crypto/pbkdf2"
)

type encryption struct {
	base utahfs.ObjectStorage
	aead cipher.AEAD
}

// WithEncryption wraps an ObjectStorage implementation and makes sure that all
// values are encrypted with AES-GCM before being processed further.
//
// The encryption key is derived with PBKDF2 from `password`.
func WithEncryption(base utahfs.ObjectStorage, password string) (utahfs.ObjectStorage, error) {
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

func (e *encryption) Get(ctx context.Context, key string) ([]byte, error) {
	raw, err := e.base.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	ns := e.aead.NonceSize()
	if len(raw) < ns {
		return nil, fmt.Errorf("storage: ciphertext is too small")
	}
	return e.aead.Open(nil, raw[:ns], raw[ns:], []byte(key))
}

func (e *encryption) Set(ctx context.Context, key string, data []byte) error {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	ct := e.aead.Seal(nil, nonce, data, []byte(key))

	return e.base.Set(ctx, key, append(nonce, ct...))
}

func (e *encryption) Delete(ctx context.Context, key string) error {
	return e.base.Delete(ctx, key)
}
