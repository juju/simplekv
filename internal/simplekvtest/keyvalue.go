// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package simplekvtest

import (
	"context"
	"time"

	gc "gopkg.in/check.v1"
	errgo "gopkg.in/errgo.v1"

	"github.com/juju/simplekv"
)

// KeyValueSuite contains a set of tests for simplekv.Store implementations. The
// NewStore parameter must be set before calling SetUpTest.
type KeyValueSuite struct {
	NewStore func() (simplekv.Store, error)
}

func (s *KeyValueSuite) SetUpSuite(c *gc.C) {}

func (s *KeyValueSuite) TearDownSuite(c *gc.C) {}

func (s *KeyValueSuite) SetUpTest(c *gc.C) {}

func (s *KeyValueSuite) TearDownTest(c *gc.C) {}

func (s *KeyValueSuite) TestSet(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, gc.Equals, nil)

	result, err := kv.Get(ctx, "test-key")
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(result), gc.Equals, "test-value")

	// Try again with an existing record, which might trigger different behavior.
	err = kv.Set(ctx, "test-key", []byte("test-value-2"), time.Time{})
	c.Assert(err, gc.Equals, nil)

	result, err = kv.Get(ctx, "test-key")
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(result), gc.Equals, "test-value-2")
}

func (s *KeyValueSuite) TestGetNotFound(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	_, err := kv.Get(ctx, "test-not-there-key")
	c.Assert(errgo.Cause(err), gc.Equals, simplekv.ErrNotFound)
	c.Assert(err, gc.ErrorMatches, "key test-not-there-key not found")
}

func (s *KeyValueSuite) TestSetKeyOnce(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := simplekv.SetKeyOnce(ctx, kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, gc.Equals, nil)

	result, err := kv.Get(ctx, "test-key")
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(result), gc.Equals, "test-value")
}

func (s *KeyValueSuite) TestSetKeyOnceDuplicate(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := simplekv.SetKeyOnce(ctx, kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, gc.Equals, nil)

	err = simplekv.SetKeyOnce(ctx, kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(errgo.Cause(err), gc.Equals, simplekv.ErrDuplicateKey)
	c.Assert(err, gc.ErrorMatches, "key test-key already exists")
}

func (s *KeyValueSuite) TestUpdateSuccessWithPreexistingKey(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()
	err := kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, gc.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), gc.Equals, "test-value")
		return []byte("test-value-2"), nil
	})
	c.Assert(err, gc.Equals, nil)

	val, err := kv.Get(ctx, "test-key")
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(val), gc.Equals, "test-value-2")
}

func (s *KeyValueSuite) TestUpdateSuccessWithoutPreexistingKey(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, gc.IsNil)
		return []byte("test-value"), nil
	})
	c.Assert(err, gc.Equals, nil)

	val, err := kv.Get(ctx, "test-key")
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(val), gc.Equals, "test-value")
}

func (s *KeyValueSuite) TestUpdateConcurrent(c *gc.C) {
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
				c.Check(err, gc.Equals, nil)
			}
			done <- struct{}{}
		}()
	}
	<-done
	<-done
	val, err := kv.Get(ctx, "test-key")
	c.Assert(err, gc.Equals, nil)
	c.Assert(val, gc.HasLen, 1)
	c.Assert(int(val[0]), gc.Equals, N*2)
}

func (s *KeyValueSuite) TestUpdateErrorWithExistingKey(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	testErr := errgo.Newf("test error")

	err := kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, gc.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), gc.Equals, "test-value")
		return nil, testErr
	})
	c.Check(errgo.Cause(err), gc.Equals, testErr)

}

func (s *KeyValueSuite) TestUpdateErrorWithNonExistentKey(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	testErr := errgo.Newf("test error")

	err := kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, gc.IsNil)
		return nil, testErr
	})
	c.Check(errgo.Cause(err), gc.Equals, testErr)

}

func (s *KeyValueSuite) TestSetNilUpdatesAsNonNil(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := kv.Set(ctx, "test-key", nil, time.Time{})
	c.Assert(err, gc.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Assert(oldVal, gc.DeepEquals, []byte{})
		return nil, nil
	})
	c.Assert(err, gc.Equals, nil)
}

func (s *KeyValueSuite) TestUpdateReturnNilThenUpdatesAsNonNil(c *gc.C) {
	kv, ctx, close := s.newStore(c)
	defer close()

	err := kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, gc.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), gc.Equals, "test-value")
		return nil, nil
	})
	c.Assert(err, gc.Equals, nil)

	err = kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, gc.NotNil)
		c.Assert(oldVal, gc.DeepEquals, []byte{})
		return nil, nil
	})
	c.Assert(err, gc.Equals, nil)
}

func (s *KeyValueSuite) newStore(c *gc.C) (simplekv.Store, context.Context, func()) {
	kv, err := s.NewStore()
	c.Assert(err, gc.Equals, nil)
	ctx, close := kv.Context(context.Background())
	return kv, ctx, close
}
