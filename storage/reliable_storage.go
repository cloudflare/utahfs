package storage

import (
	"context"
)

type simpleReliableStorage struct {
	base ObjectStorage
}

// NewSimpleReliableStorage returns a ReliableStorage implementation, intended
// for testing. It simply panics if the atomicity of a transaction is broken.
func NewSimpleReliableStorage(base ObjectStorage) ReliableStorage {
	return &simpleReliableStorage{base}
}

func (srs *simpleReliableStorage) Start(ctx context.Context) error { return nil }

func (srs *simpleReliableStorage) Get(ctx context.Context, key string) ([]byte, error) {
	return srs.base.Get(ctx, key)
}

func (srs *simpleReliableStorage) Commit(ctx context.Context, writes map[string][]byte) error {
	for key, val := range writes {
		if err := srs.base.Set(ctx, key, val); err != nil {
			panic(err)
		}
	}
	return nil
}

type reliableStub struct {
	base ReliableStorage
	data map[string][]byte
}

var _ ObjectStorage = reliableStub{}

func (rs reliableStub) Get(ctx context.Context, key string) ([]byte, error) {
	raw, ok := rs.data[key]
	if !ok {
		return rs.base.Get(ctx, key)
	} else if raw == nil {
		return nil, ErrObjectNotFound
	}
	return dup(raw), nil
}

func (rs reliableStub) Set(ctx context.Context, key string, data []byte) error {
	rs.data[key] = dup(data)
	return nil
}

func (rs reliableStub) Delete(ctx context.Context, key string) error {
	rs.data[key] = nil
	return nil
}

// NOTE: This code is commented out because I'm not sure that it's valuable, but
// I spent a lot of time on it and don't want to delete it.
//
// // ReliableWrapper provides a way for ObjectStorage implementations that add
// // additional functionality to be composed in front of a ReliableStorage
// // implementation.
// //
// // Take the Store field of the exposed struct, set it as the base storage of any
// // other ObjectStorage implementations, and then replace it with the result. For
// // example:
// //   ros := storage.NewReliableWrapper(base)
// //   ros.Store, _ = storage.NewCache(ros.Store, 4096)
// // and now any reads will possibly be answered from cached before hitting
// // `base`, and any redundant writes will be omitted.
// type ReliableWrapper struct {
// 	Store ObjectStorage
// 	stub  reliableStub
// }
//
// var _ ReliableStorage = &ReliableWrapper{}
//
// func NewReliableWrapper(base ReliableStorage) *ReliableWrapper {
// 	stub := reliableStub{
// 		base: base,
// 		data: make(map[string][]byte),
// 	}
// 	return &ReliableWrapper{
// 		Store: stub,
// 		stub:  stub,
// 	}
// }
//
// func (rw *ReliableWrapper) Start(ctx context.Context) error {
// 	return rw.stub.base.Start(ctx)
// }
//
// func (rw *ReliableWrapper) Get(ctx context.Context, key string) ([]byte, error) {
// 	return rw.Store.Get(ctx, key)
// }
//
// func (rw *ReliableWrapper) Commit(ctx context.Context, writes map[string][]byte) error {
// 	for key, val := range writes {
// 		if val == nil {
// 			if err := rw.Store.Delete(ctx, key); err != nil {
// 				return err
// 			}
// 		} else {
// 			if err := rw.Store.Set(ctx, key, val); err != nil {
// 				return err
// 			}
// 		}
// 	}
//
// 	finalWrites := make(map[string][]byte)
// 	for key, val := range rw.stub.data {
// 		finalWrites[key] = val
// 		delete(rw.stub.data, key)
// 	}
//
// 	return rw.stub.base.Commit(ctx, finalWrites)
// }
