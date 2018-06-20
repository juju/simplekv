// Copyright 2018 Canonical Ltd.
// Licensed under the LGPL, see LICENCE file for details.

package simplekv

import (
	"context"
	"time"

	errgo "gopkg.in/errgo.v1"
)

var (
	// ErrNotFound is the error cause used when an identity cannot be
	// found in storage.
	ErrNotFound = errgo.New("not found")

	// ErrDuplicateKey is the error cause used when SetKeyOnce
	// tries to set a duplicate key.
	ErrDuplicateKey = errgo.New("duplicate key")
)

// KeyNotFoundError creates a new error with a cause of ErrNotFound and
// an appropriate message.
func KeyNotFoundError(key string) error {
	err := errgo.WithCausef(nil, ErrNotFound, "key %s not found", key)
	err.(*errgo.Err).SetLocation(1)
	return err
}

// Store holds the interface implemented by the various backend implementations.
type Store interface {
	// Context returns a context that is suitable for passing to the
	// other Store methods. Store methods called with
	// such a context will be sequentially consistent; for example, a
	// value that is set in Set will immediately be available from
	// Get.
	//
	// The returned close function must be called when the returned
	// context will no longer be used, to allow for any required
	// cleanup.
	Context(ctx context.Context) (_ context.Context, close func())

	// Get retrieves the value associated with the given key. If
	// there is no such key an error with a cause of ErrNotFound will
	// be returned.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set updates the given key to have the specified value.
	//
	// If the expire time is non-zero then the entry may be garbage
	// collected at some point after that time. Clients should not
	// rely on the value being removed at the given time.
	Set(ctx context.Context, key string, value []byte, expire time.Time) error

	// Update updates the value for the given key. The getVal
	// function is called with the old value of the key and should
	// return the new value, which will be updated atomically;
	// getVal may be called several times, so should not have
	// side-effects.
	//
	// If an entry for the given key did not previously exist, old
	// will be nil.
	//
	// If getVal returns an error, it will be returned by Update with
	// its cause unchanged.
	//
	// If the expire time is non-zero then the entry may be garbage
	// collected at some point after that time. Clients should not
	// rely on the value being removed at the given time.
	Update(ctx context.Context, key string, expire time.Time, getVal func(old []byte) ([]byte, error)) error
}

// SetKeyOnce is like Store.Set except that if the key already
// has a value associated with it it returns an error with a cause of
// ErrDuplicateKey.
func SetKeyOnce(ctx context.Context, kv Store, key string, value []byte, expire time.Time) error {
	err := kv.Update(ctx, key, expire, func(old []byte) ([]byte, error) {
		if old != nil {
			return nil, errgo.WithCausef(nil, ErrDuplicateKey, "key %s already exists", key)
		}
		return value, nil
	})
	return errgo.Mask(err, errgo.Is(ErrDuplicateKey))
}
