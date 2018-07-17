// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package simplekvtest

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	errgo "gopkg.in/errgo.v1"

	"github.com/juju/simplekv"
)

type suite struct {
	newStore func(c *qt.C) (simplekv.Store, context.Context, func())
}

// TestStore runs a set of tests to check that a given
// store implementation works correctly. The newStore
// function will be called to create new store instances
// for the tests - each one should be independent
// of the others.
func TestStore(t *testing.T, newStore func() (_ simplekv.Store, err error)) {
	s := suite{
		newStore: func(c *qt.C) (simplekv.Store, context.Context, func()) {
			kv, err := newStore()
			c.Assert(err, qt.Equals, nil)
			ctx, close := kv.Context(context.Background())
			return kv, ctx, close
		},
	}
	tests := []struct {
		name string
		f    func(c *qt.C)
	}{
		{"Set", s.TestSet},
		{"GetNotFound", s.TestGetNotFound},
		{"SetKeyOnce", s.TestSetKeyOnce},
		{"SetKeyOnceDuplicate", s.TestSetKeyOnceDuplicate},
		{"UpdateSuccessWithPreexistingKey", s.TestUpdateSuccessWithPreexistingKey},
		{"UpdateSuccessWithoutPreexistingKey", s.TestUpdateSuccessWithoutPreexistingKey},
		{"UpdateConcurrent", s.TestUpdateConcurrent},
		{"UpdateErrorWithExistingKey", s.TestUpdateErrorWithExistingKey},
		{"UpdateErrorWithNonExistentKey", s.TestUpdateErrorWithNonExistentKey},
		{"SetNilUpdatesAsNonNil", s.TestSetNilUpdatesAsNonNil},
		{"UpdateReturnNilThenUpdatesAsNonNil", s.TestUpdateReturnNilThenUpdatesAsNonNil},
	}
	c := qt.New(t)
	for _, test := range tests {
		c.Run(test.name, test.f)
	}
}

func (s suite) TestSet(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	result, err := kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(result), qt.Equals, "test-value")

	// Try again with an existing record, which might trigger different behavior.
	err = kv.Set(ctx, "test-key", []byte("test-value-2"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	result, err = kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(result), qt.Equals, "test-value-2")
}

func (s suite) TestGetNotFound(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	_, err := kv.Get(ctx, "test-not-there-key")
	c.Assert(errgo.Cause(err), qt.Equals, simplekv.ErrNotFound)
	c.Assert(err, qt.ErrorMatches, "key test-not-there-key not found")
}

func (s suite) TestSetKeyOnce(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := simplekv.SetKeyOnce(ctx, kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	result, err := kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(result), qt.Equals, "test-value")
}

func (s suite) TestSetKeyOnceDuplicate(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := simplekv.SetKeyOnce(ctx, kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = simplekv.SetKeyOnce(ctx, kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(errgo.Cause(err), qt.Equals, simplekv.ErrDuplicateKey)
	c.Assert(err, qt.ErrorMatches, "key test-key already exists")
}

func (s suite) TestUpdateSuccessWithPreexistingKey(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()
	err := kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), qt.Equals, "test-value")
		return []byte("test-value-2"), nil
	})
	c.Assert(err, qt.Equals, nil)

	val, err := kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(val), qt.Equals, "test-value-2")
}

func (s suite) TestUpdateSuccessWithoutPreexistingKey(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, qt.IsNil)
		return []byte("test-value"), nil
	})
	c.Assert(err, qt.Equals, nil)

	val, err := kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(val), qt.Equals, "test-value")
}

func (s suite) TestUpdateConcurrent(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	const N = 100
	done := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			for j := 0; j < N; j++ {
				err := kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
					time.Sleep(time.Millisecond)
					if oldVal == nil {
						return []byte{1}, nil
					}
					return []byte{oldVal[0] + 1}, nil
				})
				c.Check(err, qt.Equals, nil)
			}
			done <- struct{}{}
		}()
	}
	<-done
	<-done
	val, err := kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(val, qt.HasLen, 1)
	c.Assert(int(val[0]), qt.Equals, N*2)
}

func (s suite) TestUpdateErrorWithExistingKey(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	testErr := errgo.Newf("test error")

	err := kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), qt.Equals, "test-value")
		return nil, testErr
	})
	c.Check(errgo.Cause(err), qt.Equals, testErr)

}

func (s suite) TestUpdateErrorWithNonExistentKey(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	testErr := errgo.Newf("test error")

	err := kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, qt.IsNil)
		return nil, testErr
	})
	c.Check(errgo.Cause(err), qt.Equals, testErr)

}

func (s suite) TestSetNilUpdatesAsNonNil(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := kv.Set(ctx, "test-key", nil, time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Assert(oldVal, qt.DeepEquals, []byte{})
		return nil, nil
	})
	c.Assert(err, qt.Equals, nil)
}

func (s suite) TestUpdateReturnNilThenUpdatesAsNonNil(c *qt.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), qt.Equals, "test-value")
		return nil, nil
	})
	c.Assert(err, qt.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, qt.Not(qt.IsNil))
		c.Assert(oldVal, qt.DeepEquals, []byte{})
		return nil, nil
	})
	c.Assert(err, qt.Equals, nil)
}
