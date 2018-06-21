// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package memsimplekv

import (
	"context"
	"sync"
	"time"

	errgo "gopkg.in/errgo.v1"

	"github.com/juju/simplekv"
)

// NewStore returns a new Store instance.
func NewStore() simplekv.Store {
	return &kvStore{
		data: make(map[string][]byte),
	}
}

type kvStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

// Context implements simplekv.Store.Context by returning the given
// context unchanged and a nop close function.
func (s *kvStore) Context(ctx context.Context) (_ context.Context, close func()) {
	return ctx, func() {}
}

// Get implements simplekv.Store.Get.
func (s *kvStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		return nil, simplekv.KeyNotFoundError(key)
	}
	return v, nil
}

// Set implements simplekv.Store.Set.
func (s *kvStore) Set(_ context.Context, key string, value []byte, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value == nil {
		value = []byte{}
	}
	s.data[key] = value
	return nil
}

// Update implements simplekv.Store.Update.
func (s *kvStore) Update(ctx context.Context, key string, expire time.Time, getVal func(old []byte) ([]byte, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	newVal, err := getVal(s.data[key])
	if err != nil {
		return errgo.Mask(err, errgo.Any)
	}
	if newVal == nil {
		newVal = []byte{}
	}
	s.data[key] = newVal
	return nil
}
