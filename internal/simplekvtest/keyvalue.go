// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package simplekvtest

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	errgo "gopkg.in/errgo.v1"

	"github.com/juju/simplekv"
)

// TestStore runs a set of tests to check that a given
// store implementation works correctly. The newStore
// function will be called to create new store instances
// for the tests - each one should be independent
// of the others.
func TestStore(t *testing.T, newStore func() (_ simplekv.Store, err error)) {
	runTests(qt.New(t), &suite{
		newStore: newStore,
	})
}

type suite struct {
	newStore func() (_ simplekv.Store, err error)

	ctx          context.Context
	closeContext func()

	kv simplekv.Store
}

func (s *suite) SetUpTest(c *qt.C) {
	var err error
	s.kv, err = s.newStore()
	c.Assert(err, qt.Equals, nil)
	s.ctx, s.closeContext = s.kv.Context(context.Background())
}

func (s *suite) TearDownTest(c *qt.C) {
	s.closeContext()
}

func (s *suite) TestSet(c *qt.C) {
	ctx := s.ctx
	err := s.kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	result, err := s.kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(result), qt.Equals, "test-value")

	// Try again with an existing record, which might trigger different behavior.
	err = s.kv.Set(ctx, "test-key", []byte("test-value-2"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	result, err = s.kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(result), qt.Equals, "test-value-2")
}

func (s *suite) TestGetNotFound(c *qt.C) {
	ctx := s.ctx
	_, err := s.kv.Get(ctx, "test-not-there-key")
	c.Assert(errgo.Cause(err), qt.Equals, simplekv.ErrNotFound)
	c.Assert(err, qt.ErrorMatches, "key test-not-there-key not found")
}

func (s *suite) TestSetKeyOnce(c *qt.C) {
	ctx := s.ctx
	err := simplekv.SetKeyOnce(ctx, s.kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	result, err := s.kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(result), qt.Equals, "test-value")
}

func (s *suite) TestSetKeyOnceDuplicate(c *qt.C) {
	ctx := s.ctx
	err := simplekv.SetKeyOnce(ctx, s.kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = simplekv.SetKeyOnce(ctx, s.kv, "test-key", []byte("test-value"), time.Time{})
	c.Assert(errgo.Cause(err), qt.Equals, simplekv.ErrDuplicateKey)
	c.Assert(err, qt.ErrorMatches, "key test-key already exists")
}

func (s *suite) TestUpdateSuccessWithPreexistingKey(c *qt.C) {
	ctx := s.ctx
	err := s.kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = s.kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), qt.Equals, "test-value")
		return []byte("test-value-2"), nil
	})
	c.Assert(err, qt.Equals, nil)

	val, err := s.kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(val), qt.Equals, "test-value-2")
}

func (s *suite) TestUpdateSuccessWithoutPreexistingKey(c *qt.C) {
	ctx := s.ctx
	err := s.kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, qt.IsNil)
		return []byte("test-value"), nil
	})
	c.Assert(err, qt.Equals, nil)

	val, err := s.kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(string(val), qt.Equals, "test-value")
}

func (s *suite) TestUpdateConcurrent(c *qt.C) {
	ctx := s.ctx
	const N = 100
	done := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			for j := 0; j < N; j++ {
				err := s.kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
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
	val, err := s.kv.Get(ctx, "test-key")
	c.Assert(err, qt.Equals, nil)
	c.Assert(val, qt.HasLen, 1)
	c.Assert(int(val[0]), qt.Equals, N*2)
}

func (s *suite) TestUpdateErrorWithExistingKey(c *qt.C) {
	ctx := s.ctx
	testErr := errgo.Newf("test error")

	err := s.kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = s.kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), qt.Equals, "test-value")
		return nil, testErr
	})
	c.Check(errgo.Cause(err), qt.Equals, testErr)

}

func (s *suite) TestUpdateErrorWithNonExistentKey(c *qt.C) {
	ctx := s.ctx
	testErr := errgo.Newf("test error")

	err := s.kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, qt.IsNil)
		return nil, testErr
	})
	c.Check(errgo.Cause(err), qt.Equals, testErr)

}

func (s *suite) TestSetNilUpdatesAsNonNil(c *qt.C) {
	ctx := s.ctx
	err := s.kv.Set(ctx, "test-key", nil, time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = s.kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Assert(oldVal, qt.DeepEquals, []byte{})
		return nil, nil
	})
	c.Assert(err, qt.Equals, nil)
}

func (s *suite) TestUpdateReturnNilThenUpdatesAsNonNil(c *qt.C) {
	ctx := s.ctx
	err := s.kv.Set(ctx, "test-key", []byte("test-value"), time.Time{})
	c.Assert(err, qt.Equals, nil)

	err = s.kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(string(oldVal), qt.Equals, "test-value")
		return nil, nil
	})
	c.Assert(err, qt.Equals, nil)

	err = s.kv.Update(ctx, "test-key", time.Time{}, func(oldVal []byte) ([]byte, error) {
		c.Check(oldVal, qt.Not(qt.IsNil))
		c.Assert(oldVal, qt.DeepEquals, []byte{})
		return nil, nil
	})
	c.Assert(err, qt.Equals, nil)
}

// TODO factor the runTests function into a separate public repo somewhere.

// runTests runs all methods on the given value that have the
// prefix "Test". The signature of the test methods must be
// func(*quicktest.C).
//
// If s is is a pointer, the value pointed to is copied
// before any methods are invoked on it; a new copy
// is made for each test.
//
// If there is a method named SetUpTest, it will be
// invoked before each test method runs.
//
// If there is a method named TearDownTest, it will
// be invoked after each test method runs.
//
// If present the signature of both SetUpTest and TearDownTest
// must be func(*quicktest.C).
func runTests(c *qt.C, s interface{}) {
	sv := reflect.ValueOf(s)
	st := sv.Type()
	for i := 0; i < st.NumMethod(); i++ {
		sv := sv
		if st.Kind() == reflect.Ptr {
			// Make a copy (this makes it possible to have
			// parallel tests).
			sv1 := reflect.New(st.Elem())
			sv1.Elem().Set(sv.Elem())
			sv = sv1
		}
		methodName := st.Method(i).Name
		name := strings.TrimPrefix(methodName, "Test")
		if len(name) == len(methodName) {
			continue
		}
		c.Run(name, func(c *qt.C) {
			args := []reflect.Value{reflect.ValueOf(c)}
			if setUp := sv.MethodByName("SetUpTest"); setUp.IsValid() {
				setUp.Call(args)
			}
			if tearDown := sv.MethodByName("TearDownTest"); tearDown.IsValid() {
				defer tearDown.Call(args)
			}
			sv.Method(i).Call(args)
		})
	}
}
