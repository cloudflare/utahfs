package persistent

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/hkdf"
)

type encryption struct {
	base BlockStorage
	key  []byte
}

// WithEncryption wraps a BlockStorage implementation and makes sure that all
// values are encrypted with AES-GCM before being processed further.
//
// The encryption key is derived with Argon2 from `password`.
func WithEncryption(base BlockStorage, password string) BlockStorage {
	// NOTE: The fixed salt to Argon2 is intentional. Its purpose is domain
	// separation, not to frustrate a password cracker.
	key := argon2.IDKey([]byte(password), []byte("7fedd6d671beec56"), 1, 64*1024, 4, 32)

	return &encryption{base, key}
}

func (e *encryption) encrypt(ptr uint64, data []byte) ([]byte, error) {
	tag := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tag, ptr)
	tag = tag[:n]

	// Compute the encryption key for this block.
	key := make([]byte, 32)
	_, err := io.ReadFull(hkdf.Expand(sha256.New, e.key, tag), key)
	if err != nil {
		return nil, err
	}

	// Initialize the AEAD.
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Generate a fresh nonce and encrypt the given data.
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ct := aead.Seal(nil, nonce, data, tag)

	return append(nonce, ct...), nil
}

func (e *encryption) decrypt(ptr uint64, raw []byte) ([]byte, error) {
	tag := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tag, ptr)
	tag = tag[:n]

	// Compute the encryption key for this block.
	key := make([]byte, 32)
	_, err := io.ReadFull(hkdf.Expand(sha256.New, e.key, tag), key)
	if err != nil {
		return nil, err
	}

	// Initialize the AEAD.
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Decrypt the given data.
	ns := aead.NonceSize()
	if len(raw) < ns {
		return nil, fmt.Errorf("ciphertext is too small")
	}
	val, err := aead.Open(nil, raw[:ns], raw[ns:], tag)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (e *encryption) Start(ctx context.Context, prefetch []uint64) (map[uint64][]byte, error) {
	data, err := e.base.Start(ctx, prefetch)
	if err != nil {
		return nil, err
	}

	out := make(map[uint64][]byte)
	for ptr, raw := range data {
		val, err := e.decrypt(ptr, raw)
		if err != nil {
			return nil, fmt.Errorf("encryption: failed to decrypt block %x: %v", ptr, err)
		}
		out[ptr] = val
	}
	return out, nil
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

	out := make(map[uint64][]byte)
	for ptr, raw := range data {
		val, err := e.decrypt(ptr, raw)
		if err != nil {
			return nil, fmt.Errorf("encryption: failed to decrypt block %x: %v", ptr, err)
		}
		out[ptr] = val
	}
	return out, nil
}

func (e *encryption) Set(ctx context.Context, ptr uint64, data []byte, dt DataType) error {
	ct, err := e.encrypt(ptr, data)
	if err != nil {
		return fmt.Errorf("encryption: failed to encrypt: %v", err)
	}
	return e.base.Set(ctx, ptr, ct, dt)
}

func (e *encryption) Commit(ctx context.Context) error { return e.base.Commit(ctx) }
func (e *encryption) Rollback(ctx context.Context)     { e.base.Rollback(ctx) }
